# Ôn tập và luyện bảo vệ đồ án

Chuẩn bị từ source và artifact đã đọc ngày 25/10/2026. Không hứa điểm dự án;
rubric trong [tổng quan](system-overview-explained.md) dùng trọng số người dùng cung cấp.
Đọc [record walkthrough](data-quality-explained.md) trước khi học thuộc lời nói.

## 1. Bài trình bày khoảng 3 phút

Mốc thời gian là nhịp gợi ý để luyện, chưa được đo bằng buổi thuyết trình thật.
Khi luyện hãy bấm giờ và rút bớt câu, giữ nguyên các ranh giới bằng chứng.

**0:00–0:35 — Bài toán và phạm vi**

“Nhóm em triển khai quy trình Storage → Processing cho tám CSV network flow
CICIDS2017. Bài toán là đưa dữ liệu đã lưu về một cấu trúc sử dụng được, kiểm soát
chất lượng, tổng hợp và lưu kết quả có thể đọc lại. Phạm vi này không bao gồm mô
hình dự đoán hay cảnh báo. Đặc trưng Big Data thể hiện ở khoảng 2,83 triệu flow
và đặc biệt là Veracity: dữ liệu có Infinity, số âm, sentinel và header trùng.”

**0:35–1:10 — Kiến trúc**

“MinIO giữ raw và output qua giao diện object storage. Spark đọc bằng Hadoop S3A.
Docker Compose tổ chức Master, hai Worker và MinIO trên cùng máy. Driver chạy
trong container master vì nhóm gọi Python CLI ở đó; driver khác master daemon.
Master cấp tài nguyên, worker khởi executor, executor thực thi task. Nhóm kiểm
master URL, topology và tài nguyên application, không suy distributed chỉ từ tên container.”

**1:10–2:05 — Pipeline và chất lượng**

“Entry point chính là processing.main. Reader kiểm 79 header theo vị trí, giữ hai
header trùng bằng alias riêng, parse từng physical line và lưu evidence. Sau đó
chọn 22 trường lõi, ép kiểu, gán VALID, QUARANTINED hoặc REJECTED. Rate Infinity
được đổi thành NULL và gắn cờ; duration âm được cách ly. Nhãn và CSV gốc được giữ.
Hậu kiểm xác minh schema, cờ, nhãn và đối soát multiset để giữ đúng cả duplicate.
Từ tập VALID, nhóm tính tổng packet, payload bytes và tỷ lệ hai hướng; thống kê
chất lượng tính trên mọi trạng thái. Kết quả ghi Parquet Snappy rồi đọc lại kiểm
schema và count. Chỉ marker JSON cuối mới xác nhận toàn batch hoàn thành.”

**2:05–3:00 — Bằng chứng và thảo luận**

“Trên tám CSV thật, job chỉ đọc đã xử lý 2.830.743 dòng, gồm 2.830.628 VALID và
115 quarantine, không reject; hậu kiểm PASS. Full real output MinIO chưa được
thực hiện. Đường MinIO end-to-end được chứng minh bằng synthetic, với 22 batch
và 110 output datasets đọc lại; có 36 test PASS. Benchmark có hai lần đo mỗi cấu
hình: hai workers không nhanh hơn, speedup chỉ 0,916 lần; 16 shuffle partitions
có median thấp nhất trong bốn mức đã thử. Nhóm công bố workload nhỏ, dùng dữ liệu
nhân bản và giới hạn số lần đo, nên không suy ra cấu hình tối ưu tổng quát.”

Nguồn cho lời trình bày: [main:34](../processing/main.py#L34),
[validation:138](../processing/validation.py#L138),
[output:79](../processing/output.py#L79),
[full-real report](pipeline-eight-real.local.json),
[test-results](benchmarks/20261021/test-results.json),
[summary](benchmarks/20261021/summary.csv).

## 2. Bài trình bày khoảng 7 phút

Đây là bản nói sâu hơn, có điểm dừng chỉ sơ đồ/artifact. Thời lượng cần được luyện
thực tế; không gọi các mốc này là processing time.

**0:00–0:50 — Đặt bài toán đúng môn học**

“Đầu vào là tám file CSV đã trích xuất network flow. Mỗi row là một quan sát flow,
không phải một packet. Nhóm chọn Storage → Processing: đọc từ storage, chuẩn hóa,
đảm bảo chất lượng, tạo đặc trưng dẫn xuất và tổng hợp, sau đó ghi lại dữ liệu và
báo cáo. Khoảng 884,6 triệu bytes và 2,83 triệu rows là quy mô thực của bộ này;
nhóm không gọi đó là dữ liệu TB. Điều đáng chú ý ngoài volume là veracity: có các
giá trị không hữu hạn, thời lượng âm, thiếu window và nhãn có ký tự thay thế.
Vì vậy bài toán cần policy minh bạch và khả năng truy vết.”

**0:50–1:45 — Kiến trúc và đường giao tiếp**

“MinIO là object storage, bucket chứa object key và prefix. Một volume riêng giữ
dữ liệu MinIO. Spark worker không cần chép riêng CSV vào từng container khi đọc
qua S3A. Đường upload là Boto3 dùng S3 API; đường Spark là PySpark điều khiển JVM,
Hadoop FileSystem và S3A dùng AWS Java SDK để gọi endpoint MinIO. Docker image đã
chứa Hadoop AWS và SDK phù hợp. Compose dùng service DNS minio, không dùng
localhost từ worker để chỉ storage. Driver điều phối application; master quản lý
resources; worker khởi executor. Hai workers vẫn ở cùng một máy vật lý, nên em
không tuyên bố đã kiểm chứng cluster nhiều máy hoặc storage HA.”

**1:45–2:45 — Contract và ingestion**

“Data Contract gồm Markdown quy định ý nghĩa và JSON ánh xạ vị trí. Source có
79 cột, kể cả hai Fwd Header Length. Không thể chỉ snake_case tên cột rồi hy vọng
trùng tên tự hết. Reader kiểm exact header ở driver, tính SHA-256 để ghi lineage,
sau đó dùng textFile và mapPartitionsWithIndex để parse phân tán. Mỗi physical
line là một record trong định dạng hỗ trợ; không nhận multiline CSV tổng quát.
Dòng thiếu hoặc thừa cột vẫn giữ raw text và lý do để reject. Raw nhận StringType,
đến transformation mới chọn 22 core và ép kiểu. Điều này giúp giữ nguyên token
nguồn để biết NULL đến từ đâu.”

**2:45–3:50 — Validation, cleaning và hậu kiểm**

“Int và Long phải theo grammar nguyên, vì packet count 1,5 không thể bị cắt thành
1. Try-cast xử lý overflow mà vẫn ghi cast failure. Validation gán đúng một status,
malformed ưu tiên reject; dữ liệu parse được nhưng hard error thì quarantine.
Rate Infinity là lỗi mềm theo contract: chuyển NULL và invalid flag, row có thể
vẫn VALID. Window đúng -1 là sentinel missing; -2 là lỗi cứng. Duration âm không
bị ép thành 0. Label không được sửa thành BENIGN. Sau cleaning, em kiểm lại cờ
dựa raw, schema/types, label, miền giá trị VALID và status. Đối soát không chỉ
đếm tổng: exceptAll hai chiều kiểm multiset evidence, tránh mất một row rồi nhân
đôi row khác vẫn giữ count. Policy hiện tại không thay đổi status qua cleaning.”

**3:50–4:45 — Features, aggregate và storage**

“Tổng packets bằng forward cộng backward; tổng bytes là payload bytes hai hướng,
không toàn bộ wire bytes. Hai số Long được đổi Decimal trước cộng để tránh
overflow; tỷ lệ chỉ tính khi total packets lớn hơn 0. Features chỉ xuất trên VALID.
Aggregate count và error ratio nhìn toàn annotated, còn total/average duration
chỉ lấy VALID. Bảng có bốn grouping nên không được cộng flow_count mọi hàng để
suy input. Năm output dùng Parquet Snappy, có schema và thuận tiện đọc cột. Writer
đọc lại kiểm dtype/count và phân bố nhãn của flow. Hiện chưa partitionBy hay
compaction; batch prefix không có nghĩa đã giải quyết small files.”

**4:45–5:40 — Bằng chứng chất lượng và correctness**

“Ngày 20/10, đủ tám CSV thật đã chạy chỉ đọc trên local mount, hậu kiểm không lỗi,
bảo toàn 2.830.743 rows. NaN và Infinity giảm về 0; NULL tăng do policy missing,
không phải dữ liệu bị mất. Các giá trị hard lỗi vẫn ở 115 quarantine rows. Em
phân biệt kết quả này với E2E MinIO: E2E đã chạy synthetic và ghi 22 batch, mỗi batch
năm Parquet datasets được đọc lại; audit sau đó đọc lại marker/quality/performance
JSON. Test suite có 36 test PASS, gồm codec Snappy trong footer và trường hợp lỗi
không được phát marker thành công. Đây là bằng chứng cụ thể, không phải chỉ nhìn
container đang running.”

**5:40–6:35 — Benchmark thực tế**

“Nhóm dùng fixture nhân bản thành 12k, 60k và 120k rows, cao nhất khoảng 20,34 MB.
Có một warmup mỗi invocation, hai lần đo mỗi cấu hình, lưu cả trial và môi trường.
Timer gồm đọc, checksum, QA, xử lý, ghi và read-back Parquet; loại startup Spark và
publish report cuối. Median worker một là 22,77 giây, worker hai là 24,85 giây,
speedup 0,916. Điều này cho thấy tăng worker chưa đem lại lợi ích ở workload này;
input chỉ hai partitions và có fixed overhead. 16 shuffle partitions có median
26,16 giây, thấp nhất trong 4/8/16/32; 32 lại chậm hơn. Hai repeats không đủ kết luận
16 tối ưu cho mọi tải. Input nhân bản cũng không được dùng đánh giá phát hiện tấn công.”

**6:35–7:00 — Giới hạn và kết thúc**

“Phần còn thiếu đáng chú ý là full-real output qua MinIO, saved task timeline,
small-file management và thử nghiệm workload lớn/nhiều máy. Code có final marker
để consumer chọn batch hoàn tất nhưng chưa phải transaction ACID hay exactly-once
xuyên batch. Những hướng đó cần thiết kế và kiểm chứng riêng. Trong phạm vi hiện
tại em có thể giải thích và chỉ ra source, record Before/After, report và số đo cho
đường Storage → Processing đã triển khai.”

Nguồn kỹ thuật: [reader:43](../processing/ingestion.py#L43),
[transform:8](../processing/transform.py#L8),
[reconcile:43](../processing/validation.py#L43),
[features:9](../processing/features.py#L9), [clean:41](../processing/clean.py#L41),
[benchmark analysis](benchmark-explained.md), [demo](defense-demo-script.md).

## 3. Bộ 30 câu hỏi — 10 cơ bản

**1. Hệ thống giải quyết bài toán gì?**  
Chuẩn hóa, làm sạch có policy, kiểm chất lượng, tổng hợp và lưu network flows từ
storage; không dự đoán tấn công. [main:34](../processing/main.py#L34).

**2. Một row có phải một packet không?**  
Không, là network flow features; packet counts là thuộc tính của flow.
[Contract:118](../data/schemas/feature_dictionary.md#L118).

**3. Vì sao gọi là Big Data khi dưới 1 GB?**  
Có volume hàng triệu flows và veracity, dùng kiến trúc xử lý phân tán để học và
thử scale; chưa phải minh chứng sản xuất TB. [tổng quan](system-overview-explained.md).

**4. MinIO làm gì, Spark làm gì?**  
MinIO giữ objects; Spark đọc, biến đổi/tổng hợp và ghi output.
[Compose:18](../docker/docker-compose.yml#L18), [main:65](../processing/main.py#L65).

**5. Vai trò Docker Compose là gì?**  
Khai báo services/mạng/volumes/environment; không tự bảo đảm logic dữ liệu đúng.
[Compose:1](../docker/docker-compose.yml#L1).

**6. Master, Worker, Driver, Executor khác nhau?**  
Master cấp tài nguyên, worker daemon quản lý node/khởi executor, driver điều phối
application, executor chạy task và cache. [Spark explained](spark-distributed-explained.md).

**7. Đầu vào/đầu ra có bao nhiêu cột?**  
Raw 79 String fields; reader thêm 5 source/evidence fields. Cleaned 22 core + 12
metadata; features thêm 4. [ingestion:89](../processing/ingestion.py#L89), [clean:53](../processing/clean.py#L53).

**8. Batch hay streaming?**  
Batch: đọc tập file hữu hạn, count/aggregate và ghi một batch UUID; không có stream
API. [run_pipeline:65](../processing/main.py#L65).

**9. Data Contract có tác dụng gì?**  
Thống nhất schema, ý nghĩa/đơn vị, quality policy, lineage và output; Python thực
thi một phần quy tắc, không tự đọc hiểu mọi câu Markdown. [load_contract:23](../processing/config.py#L23).

**10. CSV khác Parquet ở đâu?**  
CSV là text row tokens; Parquet typed/columnar, có metadata, phù hợp đọc vài cột và
nén. Writer chọn Snappy, có read-back. [output:79](../processing/output.py#L79).

## 4. Bộ 30 câu hỏi — 10 trung bình

**11. Vì sao raw đọc toàn String?**  
Để giữ token và nhận diện cast failures tường minh, tránh inferSchema thay policy.
[raw_schema:45](../processing/config.py#L45), [transform:30](../processing/transform.py#L30).

**12. `try_cast` thành công có nghĩa row hợp lệ?**  
Không, port 65536 cast được nhưng range sai; validation quyết định quarantine.
[validation:172](../processing/validation.py#L172).

**13. Infinity ở rate và ở IAT có cùng xử lý không?**  
Không, hai rate là soft → NULL+flag; IAT non-finite hard → quarantine.
[validation:160](../processing/validation.py#L160), [clean:45](../processing/clean.py#L45).

**14. Sao window -1 thành NULL mà -2 không?**  
-1 là sentinel được contract chấp nhận; âm khác -1 hard error.
[validation:167](../processing/validation.py#L167), [clean:50](../processing/clean.py#L50).

**15. NaN, NULL và Infinity khác nhau thế nào?**  
NULL là thiếu giá trị SQL; NaN/Infinity là floating-point values đặc biệt. Cần
`isNull`, `isnan`, abs==Inf riêng. [clean:47](../processing/clean.py#L47).

**16. Tại sao không fill error rate bằng 0?**  
0 có nghĩa đo bằng 0; thay lỗi đo bằng 0 làm sai ngữ nghĩa. NULL+flag giữ missing
và nguyên nhân, record vẫn có thể VALID. [clean:48](../processing/clean.py#L48).

**17. Label được bảo toàn ra sao?**  
Transformation không trim/rewrite label; hậu kiểm eqNullSafe với raw label, có
distribution before/after. [transform:27](../processing/transform.py#L27), [validation:90](../processing/validation.py#L90).

**18. Vì sao cần raw evidence?**  
Biết cast/clean đã thay gì và chứng minh multiset không mất/thay duplicates;
valid output vẫn chỉ có lineage batch/file, không raw struct. [transform:37](../processing/transform.py#L37), [clean:60](../processing/clean.py#L60).

**19. Derived feature chia 0 xử lý thế nào?**  
Ratio chỉ khi total_packets>0, còn lại NULL; totals dùng nguồn hợp lệ, Decimal
trước cộng để tránh Long overflow. [features:16](../processing/features.py#L16).

**20. Aggregation có bỏ nhánh lỗi không?**  
Count/quality ratios dùng mọi row; total packets/bytes và average duration chỉ VALID.
[aggregate_flows:30](../processing/features.py#L30).

## 5. Bộ 30 câu hỏi — 10 nâng cao/phản biện

**21. Count trước/sau bằng nhau có chứng minh không mất row?**  
Chưa; A,A,B có thể thành A,A,A. Code còn so raw evidence multiset bằng exceptAll
hai chiều và status transitions. [reconcile:43](../processing/validation.py#L43).

**22. Hậu kiểm PASS mà quarantine còn số âm có mâu thuẫn không?**  
Không, hard domain checks nhắm VALID; quarantine cố ý giữ lỗi. Invariant label/cờ/
status vẫn kiểm trên mọi nhánh. [post validation:69](../processing/validation.py#L69).

**23. `F.coalesce` trong validation có giảm số partition không?**  
Không, đó là hàm chọn giá trị non-NULL. DataFrame.coalesce là API khác và chưa dùng
trong writer. [validation:85](../processing/validation.py#L85).

**24. Làm sao chứng minh job chạy phân tán?**  
Kiểm spark_master, Master topology và app resources, rồi executor task metrics ở
cả hai hosts. Hai workers ALIVE đơn thuần chưa đủ. [benchmark:145](../experiments/benchmark.py#L145).

**25. Tại sao hai workers lại chậm hơn?**  
Quan sát speedup=0,916×. Workload nhỏ, 2 input partitions và shared-host overhead
là các khả năng; chưa profiling để kết luận nguyên nhân duy nhất. [worker results](benchmarks/20261021/summary.csv).

**26. 16 partitions có phải tối ưu?**  
Chỉ có median thấp nhất trong bốn mức đã đo trên 120k synthetic, AQE=false, hai
repeats; không suy optimum tổng quát. [partitions.json](benchmarks/20261021/partitions.json).

**27. Persist có ngăn OOM và cắt DAG không?**  
Không bảo đảm; nó cache materialized partitions theo storage level, không checkpoint.
DISK_ONLY vẫn cần heap khi tính/tạo cache batches. [main:47](../processing/main.py#L47).

**28. Pipeline có exactly-once hoặc transaction toàn batch không?**  
Chưa. Same batch bị chặn overwrite, new batch tạo output mới; multi-output failure
có thể để lại partial output, consumer cần final marker. [output:63](../processing/output.py#L63), [main:136](../processing/main.py#L136).

**29. Chứng minh codec và write/read đúng bằng cách nào?**  
Writer option Snappy, read-back dtypes/count/distribution; test đọc footer codec
SNAPPY. Chưa so toàn bộ numeric row content sau write. [output:79](../processing/output.py#L79), [test_pipeline:57](../tests/test_pipeline.py#L57).

**30. Kết quả đã chứng minh và phần còn thiếu quan trọng nhất?**  
Full real read-only và synthetic E2E/benchmark/tests đã có; full real output MinIO,
tải GB/multimachine và saved task timeline chưa có. [evidence summary](system-overview-explained.md).

## 6. Mười câu hỏi khó có thể bị hỏi tiếp ngay trên code

| Câu phản biện | Đáp án cốt lõi và bằng chứng |
|---|---|
| 1. Pipeline biết input gốc bao nhiêu dòng độc lập với parser không? | Main count raw_df rồi dùng làm expected. Đối soát mạnh từ raw_df trở đi; không tự join count audit độc lập. Parser/test giữ physical lines, nhưng không gọi equality này là proof độc lập của mọi nguồn. [main:67](../processing/main.py#L67) |
| 2. Tự đổi total_fwd_packets từ 3 thành 4 có bị hậu kiểm bắt không? | Nếu vẫn đúng type/domain và raw evidence không đổi, các rule hiện tại chưa so equality cho mọi non-rate; có thể lọt. Đây là giới hạn phân tích tĩnh. [validation:98](../processing/validation.py#L98) |
| 3. Output count/label đúng đã bảo đảm total_bytes đúng chưa? | Chưa; có unit tests công thức nhưng read-back không so mọi numeric cell. Cần full-row multiset/content assurance nếu scope yêu cầu. [output:88](../processing/output.py#L88) |
| 4. Vì sao schema JSON nói PERMISSIVE mà em bảo strict parser? | Metadata trong JSON không tự được reader apply. Code dùng text RDD/csv.reader(strict=True). Cần thống nhất mô tả, nói rõ code actual. [ingestion:24](../processing/ingestion.py#L24) |
| 5. Tại sao nullable schema đọc Parquet thay đổi mà không coi lỗi? | Spark có thể đọc Parquet với nullable hints nới lỏng; code so dtypes và kiểm NULL thực bằng rules, không dùng nullable hints như constraints. [schema_validation:11](../processing/validation.py#L11) |
| 6. `processed_at` có thể dùng time-window phát hiện flow trong ngày không? | Không, đó là giờ xử lý; nguồn không có event timestamp/IP. Không giả tạo event time từ processing time. [transform:50](../processing/transform.py#L50) |
| 7. `syn_flag_count=1` có nghĩa 1 packet SYN trong flow không? | Contract chỉ kiểm source 0/1; chưa xác minh ngữ nghĩa packet count, không suy số packet thật. [Contract:138](../data/schemas/feature_dictionary.md#L138) |
| 8. Source SHA metadata giống nghĩa upload bytes đã được hash remote? | Uploader kiểm HEAD metadata/length; ingestion tính content SHA là bước độc lập sau này. Chưa có atomic no-overwrite đa writer. [upload:79](../storage/upload_minio.py#L79), [ingestion:80](../processing/ingestion.py#L80) |
| 9. Tăng shuffle partitions lên 32 sao features vẫn 2? | Features là các phép row-local trên valid input; thay shuffle config không tự repartition mọi frame. [features:22](../processing/features.py#L22), [main:124](../processing/main.py#L124) |
| 10. Có 110 Parquet nghĩa 110 files không? | Là 22 batch × 5 output datasets/prefix. Mỗi prefix có thể chứa nhiều part-files, marker và metadata; chưa báo số physical files. [audit scope](benchmarks/20261021/minio-audit.json) |

## 7. Cách luyện vấn đáp trực tiếp

Chỉ bắt đầu khi người học yêu cầu, ví dụ “Bắt đầu luyện vấn đáp”. Mỗi lượt:

1. Giảng viên hỏi **một câu**, không đưa đáp án hay gợi ý dẫn tới đáp án trước.
2. Chờ sinh viên trả lời; có thể yêu cầu họ chỉ file/hàm hoặc giải thích ví dụ.
3. Chấm 0–10 cho **câu trả lời**, nêu phần đúng/sai/thiếu, đưa đáp án chuẩn sau đó.
4. Hỏi một câu tiếp theo độ khó tăng dần, ưu tiên nối vào điểm sinh viên vừa nhầm.

| Mức | Ý nghĩa điểm luyện |
|---|---|
| 0–2 | Sai bản chất, nhầm ML với processing hoặc không trả lời |
| 3–4 | Biết thuật ngữ nhưng nhầm vai trò/luồng dữ liệu |
| 5–6 | Đúng nguyên lý cơ bản, thiếu ví dụ/code/bằng chứng hoặc giới hạn |
| 7–8 | Đúng nguyên lý và hành vi repo, có ví dụ/source, phân biệt đo thật với minh họa |
| 9–10 | Chính xác, mạch lạc; tự chỉ giới hạn/counterexample và bảo vệ trade-off bằng evidence |

Không tự tăng điểm vì nói nhiều thuật ngữ. Câu “em chưa có bằng chứng task timeline,
em mới có resource allocation” chính xác hơn khẳng định vô căn cứ “chắc chắn mọi
stage chia đều cả hai workers”. Không đưa câu hỏi đầu tiên của chế độ này khi người
học chưa yêu cầu bắt đầu.
