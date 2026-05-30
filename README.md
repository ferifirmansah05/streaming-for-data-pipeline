# Real-Time Transaction Streaming Pipeline with Kafka & PySpark

Project ini mengimplementasikan sebuah pipeline data realtime menggunakan **Apache Kafka** sebagai *event broker* dan **PySpark Structured Streaming** sebagai mesin pemroses data (*streaming engine*). Pipeline ini dirancang untuk menangani pengiriman data transaksi secara real-time, melakukan validasi kualitas data secara ketat, menyaring data duplikat dan data usang (*late events*) menggunakan mekanisme *watermark*, serta memisahkan aliran data (*routing*) ke topik yang valid maupun ke *Dead Letter Queue* (DLQ) untuk keperluan audit.

---

1. **Real-Time Event Producer**: Mensimulasikan pengiriman data transaksi dalam format JSON setiap 1–2 detik dengan menyisipkan berbagai skenario anomali secara dinamis (data tidak valid, data terlambat, dan data duplikat).
2. **5 Layer Validasi**:
   - **Mandatory Field Check**: Memastikan keberadaan field utama (`user_id`, `amount`, `timestamp`).
   - **Type Validation**: Memvalidasi kesesuaian tipe data, termasuk konversi string timestamp ke tipe `Timestamp` Spark.
   - **Range Validation**: Membatasi nominal transaksi (`amount`) hanya pada rentang 1 s.d. 10.000.000.
   - **Source Validation**: Memastikan asal transaksi hanya berasal dari platform resmi (`mobile`, `web`, `pos`).
   - **Duplicate Detection**: Mendeteksi duplikasi data berdasarkan kombinasi unik `user_id` dan `timestamp`.
3. **Watermarking & Late Events Handling**: Menerapkan `.withWatermark("event_time", "3 minutes")`. Data valid yang datang terlambat lebih dari 3 menit akan otomatis dianggap kedaluwarsa oleh windowing Spark.
4. **Tumbling Window & Accumulative Monitoring**: Melakukan agregasi data valid per window waktu 1 menit dan menghitung total kumulatif transaksi (*running total*) yang diperbarui pada setiap micro-batch melalui fungsi `foreachBatch`.

---
