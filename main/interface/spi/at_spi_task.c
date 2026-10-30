/*
 * SPDX-FileCopyrightText: 2024-2026 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Apache-2.0
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "sdkconfig.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"
#include "freertos/stream_buffer.h"
#include "esp_system.h"
#include "esp_log.h"

#ifdef CONFIG_AT_BASE_ON_SPI
#include "driver/gpio.h"
#include "driver/spi_slave_hd.h"
#include "esp_at.h"
#include "esp_at_interface.h"

#define AT_SPI_DMA_SIZE                 4092
#define SLAVE_CONFIG_ADDR               4

/* 超时宏: 防止 SPI 事务永远阻塞卡死任务 */
#define SPI_QUEUE_TRANS_TIMEOUT_MS      5000    /* spi_slave_hd_queue_trans 最多等 5s */
#define SPI_GET_RES_TIMEOUT_MS          5000    /* spi_slave_hd_get_trans_res 最多等 5s */
#define SPI_XQUEUE_TIMEOUT_MS           30000   /* xQueueReceive 最多等 30s, 超时则循环继续 */

typedef enum {
    SPI_NULL = 0,
    SPI_SLAVE_WR,         // slave -> master
    SPI_SLAVE_RD,         // maste -> slave
} spi_mode_t;

typedef struct {
    spi_mode_t direct;
} spi_msg_t;

typedef struct {
    uint32_t     direct : 8;
    uint32_t     seq_num : 8;
    uint32_t     transmit_len : 16;
} spi_rd_status_opt_t;

// static variables
static uint8_t s_init_tx_flag = 0;
static QueueHandle_t s_spi_msg_queue;
static SemaphoreHandle_t s_spi_rw_sema;
/** Serializes multi-task xStreamBufferSend to TX stream buffer (must not be held during coord lock). */
static SemaphoreHandle_t s_spi_tx_ring_mutex;
static uint8_t s_spi_slave_tx_seq_num = 0;
static uint8_t s_spi_slave_rx_seq_num = 0;
static StreamBufferHandle_t s_spi_slave_rx_ring_buf = NULL;
static StreamBufferHandle_t s_spi_slave_tx_ring_buf = NULL;
static TaskHandle_t s_task_handle = NULL;

static const char *TAG = "at-spi";

static void at_spi_mutex_lock(void)
{
    while (xSemaphoreTake(s_spi_rw_sema, portMAX_DELAY) != pdPASS);
}

static void at_spi_mutex_unlock(void)
{
    xSemaphoreGive(s_spi_rw_sema);
}

static bool master_write_buffer_cb(void *arg, spi_slave_hd_event_t *event, BaseType_t *awoken)
{
    BaseType_t must_yield = false;
    spi_msg_t spi_msg = {
        .direct = SPI_SLAVE_RD,
    };

    ESP_EARLY_LOGE(TAG, "!!! master_write_buffer_cb FIRE queue=%p", (void*)s_spi_msg_queue);
    BaseType_t r = xQueueSendFromISR(s_spi_msg_queue, (void *)&spi_msg, &must_yield);
    ESP_EARLY_LOGE(TAG, "!!! xQueueSendFromISR r=%d must_yield=%d", (int)r, (int)must_yield);
    if (must_yield && awoken)
        *awoken = pdTRUE;
    return true;
}

inline static void at_spi_write_transmit_len(spi_mode_t spi_mode, uint16_t transmit_len)
{
    ESP_EARLY_LOGV(TAG, "tx status: %d, %d", (uint32_t)spi_mode, transmit_len);
    if (transmit_len > CONFIG_TX_STREAM_BUFFER_SIZE) {
        ESP_EARLY_LOGI(TAG, "too large tx len: %d", transmit_len);
        return;
    }

    spi_rd_status_opt_t rd_status_opt;
    rd_status_opt.direct = spi_mode;
    rd_status_opt.transmit_len = transmit_len;
    if (spi_mode == SPI_SLAVE_WR) {
        // slave -> master
        rd_status_opt.seq_num = ++s_spi_slave_tx_seq_num;
    } else if (spi_mode == SPI_SLAVE_RD) {
        // SPI_SLAVE_RECV   maste -> slave
        rd_status_opt.seq_num = ++s_spi_slave_rx_seq_num;
    }

    spi_slave_hd_write_buffer(SPI2_HOST, SLAVE_CONFIG_ADDR, (uint8_t *)&rd_status_opt, sizeof(spi_rd_status_opt_t));
}

static int32_t at_spi_read_data(uint8_t *data, int32_t len)
{
    if (len == 0) {
        ESP_LOGI(TAG, "read empty data");
        return 0;
    }
    ESP_LOGD(TAG, "to read len: %d", len);

    uint32_t had_read_len = 0;
    had_read_len = xStreamBufferReceive(s_spi_slave_rx_ring_buf, (void *)data, len, 0);
    if (had_read_len != len) {
        ESP_LOGD(TAG, "read len error (expect:%d, actual:%d)", len, had_read_len);
    }

    return had_read_len;
}

static int32_t at_spi_write_data(uint8_t *data, int32_t len)
{
    if (len > CONFIG_TX_STREAM_BUFFER_SIZE) {
        ESP_LOGE(TAG, "invalid len:%d", len);
        return -1;
    }

    if (len == 0) {
        ESP_LOGE(TAG, "write empty data");
        return 0;
    }
    ESP_LOGD(TAG, "to write len: %d", len);

    while (xSemaphoreTake(s_spi_tx_ring_mutex, portMAX_DELAY) != pdPASS);
    int32_t had_written_len = xStreamBufferSend(s_spi_slave_tx_ring_buf, data, len, portMAX_DELAY);
    xSemaphoreGive(s_spi_tx_ring_mutex);

    if (had_written_len != len) {
        ESP_LOGE(TAG, "stream buffer send not enough (expect:%d actual:%d)", len, had_written_len);
    }

    at_spi_mutex_lock();
    if (s_init_tx_flag == 0) {
        s_init_tx_flag = 1;
        spi_msg_t spi_msg = {
            .direct = SPI_SLAVE_WR,
        };
        if (xQueueSend(s_spi_msg_queue, (void *)&spi_msg, 0) != pdPASS) {
            ESP_LOGE(TAG, "send to msg queue error");
        }
    }
    at_spi_mutex_unlock();

    return had_written_len;
}

static int32_t at_spi_get_data_len(void)
{
    if (!s_spi_slave_rx_ring_buf) {
        return 0;
    }

    return xStreamBufferBytesAvailable(s_spi_slave_rx_ring_buf);
}

static void at_spi_task(void *params)
{
    uint8_t *buffer = (uint8_t *)malloc(AT_SPI_DMA_SIZE * sizeof(uint8_t));
    if (buffer == NULL) {
        ESP_LOGE(TAG, "malloc failed");
        return;
    }

    ESP_LOGE(TAG, "spi_task: stack_here=%p buffer=%p queue=%p rw_sema=%p",
             params, (void*)buffer, (void*)s_spi_msg_queue, (void*)s_spi_rw_sema);

    // wait for AT ready
    ulTaskNotifyTake(pdTRUE, portMAX_DELAY);
    ESP_LOGE(TAG, "spi_task: unblocked from ulTaskNotifyTake");

    spi_slave_hd_data_t *ret_trans;
    while (1) {
        memset(buffer, 0x0, AT_SPI_DMA_SIZE);
        spi_msg_t trans_msg = {0};
        memset(&trans_msg, 0x0, sizeof(spi_msg_t));

        // set handshake pin to low level
        gpio_set_level(CONFIG_SPI_HANDSHAKE_PIN, 0);

        ESP_LOGE(TAG, "spi_task: waiting xQueueReceive (queue depth=%u)",
                 uxQueueMessagesWaiting(s_spi_msg_queue));
        BaseType_t qr = xQueueReceive(s_spi_msg_queue, (void*)&trans_msg,
                                       pdMS_TO_TICKS(SPI_XQUEUE_TIMEOUT_MS));
        if (qr != pdPASS) {
            ESP_LOGE(TAG, "spi_task: xQueueReceive timeout (%us), loop continue",
                     SPI_XQUEUE_TIMEOUT_MS / 1000);
            continue;
        }
        ESP_LOGE(TAG, "spi_task: got queue msg r=%d direct=%d", (int)qr, (int)trans_msg.direct);
        spi_slave_hd_data_t slave_trans;
        memset(&slave_trans, 0x0, sizeof(spi_slave_hd_data_t));

        if (trans_msg.direct == SPI_SLAVE_RD) {
            // master -> slave (主机发 AT 命令过来)
            ESP_LOGE(TAG, "spi_task: SPI_SLAVE_RD path, buf[0..3]=%02X %02X %02X %02X",
                     buffer[0], buffer[1], buffer[2], buffer[3]);
            at_spi_write_transmit_len(SPI_SLAVE_RD, AT_SPI_DMA_SIZE);
            ESP_LOGE(TAG, "spi_task: wrote transmit_len=4092, queue RX DMA");

            slave_trans.data = buffer;
            slave_trans.len = AT_SPI_DMA_SIZE;
            esp_err_t t = spi_slave_hd_queue_trans(SPI2_HOST, SPI_SLAVE_CHAN_RX, &slave_trans,
                                                    pdMS_TO_TICKS(SPI_QUEUE_TRANS_TIMEOUT_MS));
            if (t != ESP_OK) {
                ESP_LOGE(TAG, "spi_task: queue_trans(RX) timeout/err=0x%x, skip", (unsigned)t);
                continue;
            }
            ESP_LOGE(TAG, "spi_task: RX DMA queued, raise HS");

            gpio_set_level(CONFIG_SPI_HANDSHAKE_PIN, 1);
            ESP_LOGE(TAG, "spi_task: HS=1, now get_trans_res(RX)");

            t = spi_slave_hd_get_trans_res(SPI2_HOST, SPI_SLAVE_CHAN_RX, &ret_trans,
                                            pdMS_TO_TICKS(SPI_GET_RES_TIMEOUT_MS));
            if (t != ESP_OK || !ret_trans) {
                ESP_LOGE(TAG, "spi_task: get_trans_res(RX) timeout/err=0x%x, master didn't send data", (unsigned)t);
                continue;
            }
            ESP_LOGE(TAG, "spi_task: RX DMA done trans_len=%u buf[0..3]=%02X %02X %02X %02X",
                     (unsigned)ret_trans->trans_len, buffer[0], buffer[1], buffer[2], buffer[3]);
            if (ret_trans->trans_len > CONFIG_RX_STREAM_BUFFER_SIZE || ret_trans->trans_len <= 0) {
                ESP_LOGE(TAG, "recv wrong len: %d, %d, 0x%x", ret_trans->trans_len, slave_trans.len, buffer[0]);
                continue;
            }

            xStreamBufferSend(s_spi_slave_rx_ring_buf, (void *)buffer, ret_trans->trans_len, portMAX_DELAY);

            // notify at core to recv data
            esp_at_port_recv_data_notify(ret_trans->trans_len, portMAX_DELAY);

        } else if (trans_msg.direct == SPI_SLAVE_WR) {
            // slave -> master (ESP32 有数据要发, 如 ready URC / AT 响应)
            uint32_t to_send_len = 0;
            uint32_t remain_len = xStreamBufferBytesAvailable(s_spi_slave_tx_ring_buf);
            if (remain_len > 0) {
                to_send_len = remain_len > AT_SPI_DMA_SIZE ? AT_SPI_DMA_SIZE : remain_len;
                at_spi_write_transmit_len(SPI_SLAVE_WR, to_send_len);
            } else {
                ESP_LOGD(TAG, "receive send queue but no data");
                s_init_tx_flag = 0;
                continue;
            }
            uint32_t actual_len = xStreamBufferReceive(s_spi_slave_tx_ring_buf, (void *)buffer, to_send_len, 0);
            if (to_send_len != actual_len) {
                ESP_LOGE(TAG, "read len error (expect:%d actual:%d)", to_send_len, actual_len);
                s_init_tx_flag = 0;
                continue;
            }
            slave_trans.data = (uint8_t*)buffer;
            slave_trans.len = to_send_len;
            t = spi_slave_hd_queue_trans(SPI2_HOST, SPI_SLAVE_CHAN_TX, &slave_trans,
                                          pdMS_TO_TICKS(SPI_QUEUE_TRANS_TIMEOUT_MS));
            if (t != ESP_OK) {
                ESP_LOGE(TAG, "spi_task: queue_trans(TX) timeout/err=0x%x", (unsigned)t);
                s_init_tx_flag = 0;
                continue;
            }

            // slave send done and notify master to recv
            gpio_set_level(CONFIG_SPI_HANDSHAKE_PIN, 1);

            t = spi_slave_hd_get_trans_res(SPI2_HOST, SPI_SLAVE_CHAN_TX, &ret_trans,
                                            pdMS_TO_TICKS(SPI_GET_RES_TIMEOUT_MS));
            if (t != ESP_OK || !ret_trans) {
                // 关键修复: 主机没来读 (比如第一次启动主机还没初始化), 不要永远卡死
                ESP_LOGE(TAG, "spi_task: get_trans_res(TX) timeout/err=0x%x, master didn't read", (unsigned)t);
                s_init_tx_flag = 0;
                continue;
            }

            at_spi_mutex_lock();
            remain_len = xStreamBufferBytesAvailable(s_spi_slave_tx_ring_buf);
            if (remain_len > 0) {
                spi_msg_t spi_msg = {
                    .direct = SPI_SLAVE_WR,
                };
                if (xQueueSend(s_spi_msg_queue, (void *)&spi_msg, 0) != pdPASS) {
                    ESP_LOGE(TAG, "send to msg queue error");
                    break;
                }
            } else {
                s_init_tx_flag = 0;
            }
            at_spi_mutex_unlock();

        } else {
            ESP_LOGE(TAG, "unknown direct: %d", trans_msg.direct);
            continue;
        }
    }

    free(buffer);
    vTaskDelete(NULL);
}

static void at_spi_bus_default_config(spi_bus_config_t *bus_cfg)
{
    bus_cfg->mosi_io_num = CONFIG_SPI_MOSI_PIN;
    bus_cfg->miso_io_num = CONFIG_SPI_MISO_PIN;
    bus_cfg->sclk_io_num = CONFIG_SPI_SCLK_PIN;
#ifdef CONFIG_SPI_QUAD_MODE
    bus_cfg->quadwp_io_num = CONFIG_SPI_WP_PIN;
    bus_cfg->quadhd_io_num = CONFIG_SPI_HD_PIN;
    bus_cfg->flags = SPICOMMON_BUSFLAG_QUAD;
#else
    bus_cfg->quadwp_io_num = -1;
    bus_cfg->quadhd_io_num = -1;
#endif

#ifdef CONFIG_SPI_DUAL_MODE
    bus_cfg->flags = SPICOMMON_BUSFLAG_DUAL;
#endif
    bus_cfg->max_transfer_sz = 14000;
}

static void at_spi_slot_default_config(spi_slave_hd_slot_config_t *slave_hd_cfg)
{
    slave_hd_cfg->spics_io_num = CONFIG_SPI_CS_PIN;
    slave_hd_cfg->flags = 0;
    slave_hd_cfg->mode = CONFIG_SPI_MODE;
    slave_hd_cfg->command_bits = 8;
    slave_hd_cfg->address_bits = 8;
    slave_hd_cfg->dummy_bits = 8;
    slave_hd_cfg->queue_size = 4;
    slave_hd_cfg->dma_chan = SPI_DMA_CH_AUTO;

    // master writes to shared buffer
    slave_hd_cfg->cb_config.cb_buffer_rx = master_write_buffer_cb;
    slave_hd_cfg->cb_config.cb_recv = NULL;
}

static void init_slave_hd(void)
{
    spi_bus_config_t bus_cfg = {};
    at_spi_bus_default_config(&bus_cfg);

    spi_slave_hd_slot_config_t slave_hd_cfg = {};
    at_spi_slot_default_config(&slave_hd_cfg);

    ESP_ERROR_CHECK(spi_slave_hd_init(SPI2_HOST, &bus_cfg, &slave_hd_cfg));
}

static void at_spi_init(void)
{
    ESP_LOGE(TAG, "at_spi_init: start");
    // init ring buffer, queue, and mutex
    s_spi_rw_sema = xSemaphoreCreateMutex();
    s_spi_tx_ring_mutex = xSemaphoreCreateMutex();
    s_spi_msg_queue = xQueueCreate(10, sizeof(spi_msg_t));
    s_spi_slave_rx_ring_buf = xStreamBufferCreate(CONFIG_RX_STREAM_BUFFER_SIZE, 1024);
    s_spi_slave_tx_ring_buf = xStreamBufferCreate(CONFIG_TX_STREAM_BUFFER_SIZE, 1024);
    if (!s_spi_rw_sema || !s_spi_tx_ring_mutex || !s_spi_msg_queue || !s_spi_slave_rx_ring_buf || !s_spi_slave_tx_ring_buf) {
        ESP_LOGE(TAG, "create error, free heap heap: %d", esp_get_free_heap_size());
        abort();
    }
    ESP_LOGE(TAG, "at_spi_init: queue=%p rx_buf=%p tx_buf=%p",
             (void*)s_spi_msg_queue, (void*)s_spi_slave_rx_ring_buf, (void*)s_spi_slave_tx_ring_buf);

    // init gpio
    ESP_LOGE(TAG, "at_spi_init: init gpio (HS=%u)", (unsigned)CONFIG_SPI_HANDSHAKE_PIN);
    gpio_config_t io_conf;
    io_conf.intr_type = GPIO_INTR_DISABLE;
    io_conf.mode = GPIO_MODE_OUTPUT;
    io_conf.pin_bit_mask = (1ULL << CONFIG_SPI_HANDSHAKE_PIN);
    io_conf.pull_down_en = 0;
    io_conf.pull_up_en = 0;
    gpio_config(&io_conf);
    gpio_set_level(CONFIG_SPI_HANDSHAKE_PIN, 0);

    // init spi slave driver
    ESP_LOGE(TAG, "at_spi_init: init spi (SCLK=%u MOSI=%u MISO=%u CS=%u mode=%d)",
             (unsigned)CONFIG_SPI_SCLK_PIN, (unsigned)CONFIG_SPI_MOSI_PIN,
             (unsigned)CONFIG_SPI_MISO_PIN, (unsigned)CONFIG_SPI_CS_PIN, CONFIG_SPI_MODE);
    init_slave_hd();
    ESP_LOGE(TAG, "at_spi_init: spi slave hd OK");

    // init spi slave task
    xTaskCreate(at_spi_task, "at_spi_task", 8192, NULL, 10, &s_task_handle);
    ESP_LOGE(TAG, "at_spi_init: created task=%p", (void*)s_task_handle);
}

static void at_spi_sleep_before_cb(esp_at_sleep_mode_t mode)
{
    // do something before entering light-sleep
}

static void at_spi_wakeup_before_cb(void)
{
    // do something before waking up from light-sleep
}

void at_interface_init(void)
{
    // init interface driver
    at_spi_init();

    // init interface operations
    esp_at_intf_ops_t spi_ops = {
        .read_data = at_spi_read_data,
        .write_data = at_spi_write_data,
        .get_data_length = at_spi_get_data_len,
        .wait_write_complete = NULL
    };
    at_interface_ops_init(&spi_ops);

    // init interface hooks
    esp_at_custom_ops_t spi_hooks = {
        .pre_sleep_callback = at_spi_sleep_before_cb,
        .pre_wakeup_callback = at_spi_wakeup_before_cb,
        .status_callback = NULL,
        .pre_deepsleep_callback = NULL,
        .pre_restart_callback = NULL,
        .pre_active_write_data_callback = NULL,
    };
    at_interface_hooks(&spi_hooks);
}

void at_interface_start(void)
{
    xTaskNotifyGive(s_task_handle);
}

#endif
