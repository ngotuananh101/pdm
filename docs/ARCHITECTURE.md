# Ponta Download Manager (PDM) — Kiến trúc đề xuất

> Phiên bản: 0.1 · Ngày: 2026-11-05 · Trạng thái: bản thiết kế, chưa phải mã nguồn đã triển khai.
>
> Giả định: PDM ưu tiên desktop Windows trước, thiết kế để mở rộng macOS/Linux. Ứng dụng dùng Go + Fyne, hoạt động trong **một tiến trình**. Hỗ trợ trình duyệt và các tính năng nâng cao được làm sau MVP. Các ngưỡng hiệu năng, cấu hình và lựa chọn dưới đây là đề xuất cần đo thực tế, không phải kết quả benchmark.

## 1. Mục tiêu

- Tải HTTP/HTTPS ổn định; hiển thị tiến độ, tốc độ, thời gian còn lại.
- Thêm URL, chọn thư mục và tên file; quản lý nhiều lượt tải trong hàng đợi.
- Tạm dừng/tiếp tục khi server cho phép, khôi phục trạng thái sau khi thoát hoặc lỗi.
- Tải nhiều đoạn đồng thời khi server thực sự hỗ trợ HTTP Range.
- Đóng cửa sổ về khay hệ thống, vẫn tiếp tục tải; có lệnh Thoát hoàn toàn.
- Gọn, dễ test, không yêu cầu WebView hay local HTTP API cho MVP.

### Ngoài phạm vi MVP

- BitTorrent, FTP/SFTP, HLS/DASH, tải video từ nền tảng có DRM, bắt tự động mọi link từ trình duyệt.
- Đồng bộ cloud, chạy headless/daemon, cập nhật tự động, plugin, giao diện mobile.
- Tự vượt CAPTCHA/DRM hoặc thu thập cookie trình duyệt. Các khả năng này cần thiết kế riêng và tuân thủ quyền truy cập của người dùng.

## 2. Stack

| Lớp | Đề xuất | Ghi chú |
|---|---|---|
| Ngôn ngữ | Go | Chọn phiên bản Go tương thích với Fyne và dependencies khi bắt đầu; pin trong `go.mod` và CI. |
| UI desktop | Fyne v2 | UI bằng Go, không dùng WebView; tray qua `driver/desktop`. |
| Download engine | `net/http`, `context`, `io`, `os` | Không cần server HTTP cục bộ. |
| Lưu trữ | `database/sql` + `modernc.org/sqlite` | SQLite cục bộ, driver không cần CGO; bản thân Fyne vẫn có thể cần C compiler/CGO. |
| Log | `log/slog` | Không ghi URL nhạy cảm, cookie, authorization. |
| Test | `testing`, `httptest`, `go test -race` | Test engine tách UI. |
| Tích hợp trình duyệt, hậu MVP | Extension JS/TS + Go native host | Không phải 100% Go ở phía extension. |

Fyne v2.6+ yêu cầu chuyển các cập nhật UI từ goroutine do ứng dụng tạo sang luồng UI bằng `fyne.Do`; tuân theo cấu hình migration tương ứng phiên bản Fyne đang dùng. Nguồn: [Fyne — Using Goroutines](https://docs.fyne.io/started/goroutines/).

## 3. Tổng quan

```text
┌─────────────────────────────────────────────────────────┐
│                     PDM desktop process                 │
│                                                         │
│  ┌────────────────┐     command      ┌────────────────┐ │
│  │ Fyne UI + tray │ ───────────────► │ App service    │ │
│  │ - views/dialog │ ◄─────────────── │ - use cases    │ │
│  │ - view model   │   snapshot       │ - lifecycle    │ │
│  └────────────────┘                   └───────┬────────┘ │
│                                               │          │
│                         ┌─────────────────────┼───────┐  │
│                         ▼                     ▼       ▼  │
│                  ┌────────────┐       ┌──────────┐ ┌──────┐│
│                  │ Scheduler  │       │ SQLite   │ │ FS   ││
│                  │ + workers  │       │ metadata │ │ .part││
│                  └─────┬──────┘       └──────────┘ └──────┘│
│                        │ HTTP/HTTPS                       │
└────────────────────────┼──────────────────────────────────┘
                         ▼
                    Origin servers
```

**Luồng phụ thuộc:** UI → app service → domain/interface; adapters HTTP, SQLite và filesystem triển khai interface. UI không thao tác SQL, không tự tạo worker hoặc ghi trực tiếp file. Domain không import Fyne/SQLite.

Ứng dụng một tiến trình giúp đơn giản vận hành nhưng **ẩn cửa sổ không giải phóng toàn bộ RAM UI**. Nếu sau này cần tiến trình nền siêu nhẹ, tách daemon và IPC là một thay đổi kiến trúc riêng, chỉ làm sau khi đo.

## 4. Cấu trúc thư mục mục tiêu

```text
ponta-download-manager/
├── cmd/
│   └── pdm/
│       └── main.go                  # entry point, wiring, lifecycle
├── internal/
│   ├── app/
│   │   ├── service.go               # API use-case cho UI
│   │   └── lifecycle.go             # startup, recovery, shutdown
│   ├── domain/
│   │   ├── download.go              # entity, trạng thái, invariants
│   │   ├── segment.go               # đoạn, offset và checkpoint
│   │   ├── errors.go                # lỗi theo loại
│   │   └── ports.go                 # interface repository/engine
│   ├── engine/
│   │   ├── manager.go               # điều phối 1 download
│   │   ├── probe.go                 # thăm dò tài nguyên/range
│   │   ├── worker.go                # tải 1 đoạn, ghi WriteAt
│   │   ├── retry.go                 # backoff, phân loại lỗi
│   │   ├── checkpoint.go            # phối hợp lưu offset an toàn
│   │   └── finalize.go              # hoàn tất và rename
│   ├── scheduler/
│   │   └── scheduler.go             # hàng đợi, giới hạn concurrency
│   ├── storage/
│   │   └── sqlite/
│   │       ├── db.go                # mở DB, pragmas, transaction
│   │       ├── download_repo.go
│   │       ├── segment_repo.go
│   │       └── migrations/
│   │           └── 001_init.sql
│   ├── platform/
│   │   ├── paths.go                 # thư mục dữ liệu/cache theo OS
│   │   ├── lock.go                  # single-instance lock/interface
│   │   └── filesystem.go            # tên file, rename, disk checks
│   ├── ui/
│   │   ├── app.go                   # khởi tạo Fyne và điều hướng
│   │   ├── main_window.go
│   │   ├── downloads_view.go
│   │   ├── add_dialog.go
│   │   ├── settings_view.go
│   │   ├── tray.go
│   │   └── viewmodel.go             # snapshot, format tốc độ/ETA
│   └── config/
│       └── config.go                # defaults, validate
├── assets/                          # icon, tài nguyên app
├── docs/
│   ├── ARCHITECTURE.md
│   └── decisions/                   # ADR: quyết định và lý do
├── scripts/                         # build, packaging (khi cần)
├── testdata/                        # dữ liệu phục vụ kiểm thử
├── .github/workflows/ci.yml         # thêm khi cấu hình CI
├── .gitignore
├── go.mod
├── go.sum
└── README.md
```

Đây là **cấu trúc mục tiêu**, không cần tạo tất cả file trống ngay. Bước bootstrap ở cuối tài liệu chỉ tạo những file cần để chạy giao diện khởi đầu. Không nên tạo package rỗng hoặc abstraction chưa được sử dụng.

### Nguyên tắc phụ thuộc và concurrency

- Chỉ `cmd/pdm` lắp ghép triển khai cụ thể: DB, engine, scheduler, service, UI.
- Scheduler là chủ sở hữu trạng thái chạy của job; tất cả lệnh Pause/Resume/Cancel phải được serialize theo job ID. Không chạy hai worker group cho cùng job.
- Domain model là dữ liệu; service trả **bản sao snapshot** cho UI thay vì chia sẻ map/struct đang bị ghi.
- Worker dùng `context.Context` để hủy; đợi goroutine dừng trước khi cập nhật trạng thái cuối hoặc đóng file.
- UI không chặn luồng render để chờ mạng/DB. Engine không import Fyne; service gom tiến độ và UI cập nhật bằng `fyne.Do`.

## 5. Mô hình dữ liệu

### Download

| Trường | Ý nghĩa |
|---|---|
| `id` | ID ngẫu nhiên ổn định, khóa chính |
| `url` | URL nguồn; có thể chứa thông tin nhạy cảm, xem phần bảo mật |
| `filename`, `destination_dir` | Tên đích hợp lệ và thư mục đích |
| `temp_path`, `final_path` | Tệp tạm và tệp cuối trên cùng filesystem |
| `status` | Trạng thái theo bảng bên dưới |
| `total_bytes` | Kích thước nếu biết, NULL nếu không biết |
| `downloaded_bytes` | Giá trị hiển thị/checkpoint; nguồn sự thật resume là segment đã commit |
| `etag`, `last_modified` | Validator tài nguyên nếu server cung cấp |
| `supports_range` | Kết quả xác minh thực tế, không suy luận chỉ từ HEAD |
| `error_code`, `error_message` | Lỗi đã làm sạch; không lưu secrets |
| `created_at`, `updated_at` | Timestamp UTC |

### Segment

| Trường | Ý nghĩa |
|---|---|
| `download_id`, `index` | Khóa ghép; tham chiếu download |
| `start_offset`, `end_offset` | Giới hạn đoạn tính theo byte; `end_offset` **bao gồm** byte cuối |
| `committed_bytes` | Số byte liên tục tính từ `start_offset`, đã checkpoint bền vững |
| `status` | pending/running/complete; trạng thái chạy không tồn tại qua restart |

Với kích thước `N` và `k` đoạn, các khoảng phải rời nhau và phủ đúng `[0, N-1]`. Chỉ tạo nhiều đoạn khi biết `N > 0` và đã xác minh Range. Trường hợp một luồng không biết kích thước dùng một job streaming riêng, không giả định end offset.

### Trạng thái và chuyển đổi

```text
queued ──start──► probing ──► downloading ──► finalizing ──► completed
  │                  │             │                │
  ├─cancel──► cancelled            ├─pause──► paused│
  │                  └──── error ──┴────────────────┴──► failed
  └────────────── pause ──────────► paused
paused ──resume──► queued       failed ──retry──► queued
                                  failed ──cancel conflict──► cancelled
                                  failed ──resolve conflict──► finalizing
```

- `failed ──► finalizing` chỉ dùng cho conflict resolution: `ResolveConflict` finalize file `.part` có sẵn của job `file_conflict` mà không tải lại. Nếu thiếu `.part` thì đi `finalizing ──► failed`, rồi `Retry` mới xếp hàng lại (`failed ──► queued`); không có đường `finalizing ──► queued`. Mọi chuyển trạng thái trong service đều qua một hàm duy nhất kiểm tra `domain.CanTransition`; thao tác ở trạng thái không hợp lệ bị từ chối và trạng thái không đổi.
- `failed ──► cancelled` chỉ dùng cho conflict resolution: người dùng chọn Cancel khi job `file_conflict` đang ở `failed`; `ResolveConflict(ResolutionCancel)` xóa file `.part` và chuyển job sang `cancelled`, vẫn có thể `Retry` lại (`cancelled ──► queued`).

- Các transition ghi DB nhất quán; service từ chối thao tác không hợp lệ. `completed` là terminal, trừ khi người dùng chủ động xóa bản ghi.
- `Pause` khi đang tải: phát cancel → chờ worker kết thúc → flush/checkpoint → ghi `paused`. UI có thể biểu diễn `pausing` tạm thời nhưng không được báo paused khi worker vẫn ghi.
- `Cancel`: dừng worker; mặc định **hỏi người dùng** có xóa file tạm hay không. Xóa bản ghi và xóa file là hai lựa chọn riêng; không tự xóa file hoàn tất.
- Khi startup: kiểm tra DB + file `.part`; `probing/downloading` từ phiên trước chuyển sang `paused` hoặc `queued` tùy cài đặt. Job `finalizing` cần reconciliation theo §8; không đánh dấu hoàn tất chỉ vì file có tồn tại.

## 6. Quy trình tải

1. **Create:** validate URL scheme `http/https`, tên file và thư mục đích; xác định chính sách trùng tên; lưu job `queued`.
2. **Schedule:** scheduler nhận job khi còn slot toàn cục và slot theo host; chuyển `probing`.
3. **Probe:** thử HEAD nếu hữu ích, nhưng không coi HEAD/`Accept-Ranges` là bằng chứng đủ. Khi an toàn, gửi `GET` với `Range: bytes=0-0` và `Accept-Encoding: identity`; kiểm tra `206` và `Content-Range`, tổng kích thước và validator. Đóng body probe. Nếu HEAD không được hỗ trợ, probe GET vẫn hoạt động.
4. **Plan:** nếu xác minh được Range và kích thước: chia segment, tạo file `.part` tại thư mục đích và lưu kế hoạch trước khi chạy. Nếu không, tải streaming một luồng. Nội dung chưa biết kích thước vẫn được hỗ trợ, UI hiển thị tiến độ không xác định.
5. **Execute:** mỗi worker yêu cầu đúng khoảng còn lại, xác minh status và `Content-Range`, đọc theo buffer giới hạn và `WriteAt` vào offset tương ứng. Không để hai worker ghi chồng lấn. Đóng response body và xử lý cancel.
6. **Progress:** cộng bytes đã ghi để hiển thị; snapshot gộp 250–500 ms/lần là điểm khởi đầu, không phải cam kết hiệu năng. Không gọi UI hoặc ghi SQLite cho mỗi buffer.
7. **Finalize:** sau khi toàn bộ byte cần thiết đã bền vững, đóng/sync file; kiểm tra kích thước và checksum nếu người dùng cung cấp; sau đó rename an toàn và ghi trạng thái completed.

### Quy tắc HTTP và resume

- `Range: bytes=start-end` có end inclusive. Với HTTP `206`, chỉ chấp nhận response có `Content-Range` khớp khoảng yêu cầu và tổng kích thước kỳ vọng; giới hạn số byte đọc. Xử lý server trả sai/thiếu header như lỗi, không ghi nhầm dữ liệu.
- `200` cho request có Range nghĩa là server có thể đã bỏ qua Range hoặc validator không còn khớp. **Không ghi body 200 vào vị trí segment**; dừng các segment, probe lại và chỉ chuyển sang tải một luồng mới sau khi xử lý file cũ an toàn.
- `416`: kiểm tra lại kích thước/validator; không mặc định là đã tải xong. `HEAD`, redirects và tính năng range có thể khác nhau giữa các server.
- Khi resume, dùng **strong ETag** với `If-Range`; không dùng weak ETag cho `If-Range`. Nếu không có strong ETag, chỉ dùng `Last-Modified` khi đủ điều kiện là strong validator theo HTTP; nếu không xác minh được tính bất biến, mặc định yêu cầu tải lại từ đầu hoặc xác nhận rủi ro — không ghép dữ liệu khác phiên bản.
- Giữ `Accept-Encoding: identity` và tắt nén tự động ở transport để byte range tương ứng dữ liệu ghi; kiểm tra response không đổi encoding. Lưu validator cuối sau redirect; nếu URL/validator thay đổi, dừng và xác minh lại.
- Không hứa pause/resume đối với server không hỗ trợ Range. Pause vẫn dừng tác vụ, nhưng resume có thể phải bắt đầu lại; thông báo rõ trên UI.
- HTTP 401/403: báo xác thực/quyền truy cập, không retry vô hạn. 404: báo lỗi. 429/503: backoff có jitter và tôn trọng `Retry-After` nếu hợp lệ. Timeout/network/5xx: retry hữu hạn; `context.Canceled` do pause/cancel không tính là lỗi mạng.

Tài liệu tham khảo: [MDN — Range requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Range_requests), [RFC 9110 — If-Range](https://www.rfc-editor.org/rfc/rfc9110.html#name-if-range).

## 7. Scheduler, mạng và tài nguyên

### Mặc định ban đầu để benchmark

```text
max_active_downloads   = 3     # số job đang tải
segments_per_download  = 4     # trần; chỉ khi server hỗ trợ
max_connections_host   = 4     # tổng của mọi job tới cùng host
min_segment_size       = 1 MiB # giảm số đoạn với file nhỏ
buffer_size            = 64 KiB/worker
progress_interval      = 500 ms
checkpoint_interval    = 5 s   # hoặc khi pause/shutdown; phải đảm bảo durability
max_retries            = 3     # cho lỗi tạm thời, có backoff + jitter
```

Các số trên là **giả định khởi đầu** và phải cho phép điều chỉnh. Slot theo host tính trên tổng các request, bao gồm probe; không chỉ dựa vào `http.Transport` để điều phối. Dùng một `http.Client`/`Transport` tái sử dụng, cấu hình giới hạn kết nối, dial/TLS/response-header timeout và idle connection. Không áp `http.Client.Timeout` ngắn cho toàn bộ lượt tải lớn; dùng context và cơ chế phát hiện không có tiến độ phù hợp.

`MaxIdleConnsPerHost` chỉ giới hạn kết nối **nhàn rỗi**; nếu cần giới hạn cả kết nối đang hoạt động, cân nhắc `MaxConnsPerHost` cùng semaphore của scheduler. Xem [Go `net/http` Transport](https://go.dev/src/net/http/transport.go).

## 8. Tính toàn vẹn, crash và khôi phục

### Checkpoint an toàn

File thưa/preallocate có thể chứa byte chưa tải, vì vậy **kích thước file không chứng minh nội dung hợp lệ**.

1. Worker tuần tự ghi từ đầu phần còn lại của segment; theo dõi riêng `written_bytes` trong RAM và `committed_bytes` trong DB.
2. Checkpoint theo từng nhóm: chặn/đồng bộ các write liên quan → flush/sync file thành công → ghi offsets đã sync vào một transaction SQLite → commit → mới công bố checkpoint. Không advance DB trước khi dữ liệu tương ứng được sync. Khi sync/commit lỗi, dừng job và báo lỗi.
3. Trên crash, chỉ tin `committed_bytes`; phần đã ghi sau checkpoint có thể tải lại và ghi đè. Bảo đảm tệp còn đúng kích thước/identity và validator vẫn hợp lệ. Nếu thiếu `.part` hoặc metadata bất nhất, chuyển `failed` và cho phép restart sạch.
4. Có thể đánh đổi hiệu năng bằng cách checkpoint ít hơn; cần benchmark `fsync` trên Windows/macOS/Linux. Độ bền thực tế phụ thuộc hệ điều hành, filesystem và SQLite; không cam kết chống mất điện tuyệt đối nếu chưa kiểm thử.

### Hoàn tất an toàn

- Lưu thông tin dự định hoàn tất và kiểm tra mọi segment đã commit trước khi vào `finalizing`.
- `Sync`, đóng file, xác nhận kích thước; nếu có checksum từ nguồn tin cậy thì kiểm tra. Không coi checksum tự tạo sau khi tải là xác thực nội dung từ server.
- Rename từ `.part` sang đích cùng filesystem. Mặc định **không ghi đè**: chọn tên mới hoặc yêu cầu xác nhận. Phải dùng cơ chế no-clobber phù hợp OS, không dựa vào `Stat` rồi `Rename` vì có race; nếu không hỗ trợ, báo lỗi an toàn. Không giả định rename đè có cùng semantics trên mọi OS.
- Chỉ ghi `completed` sau khi rename thành công. Khi restart giữa rename và commit, reconciliation dựa trên đường dẫn, kích thước, validator/checksum và metadata `finalizing`; nếu không thể xác minh thì yêu cầu người dùng kiểm tra, không tự xóa hay ghi đè.
- Dung lượng trống có thể thay đổi; mọi `WriteAt`, `Sync`, `Close`, transaction và rename đều phải kiểm tra lỗi. Không tự xóa file người dùng khi lỗi.

## 9. SQLite và migration

- Bắt đầu với `downloads`, `segments`, `schema_migrations`; chỉ thêm settings table khi thực sự cần. `segments.download_id` có foreign key, transaction xóa segment theo policy.
- Mở DB trong thư mục dữ liệu người dùng, bật `foreign_keys`; cân nhắc WAL và `busy_timeout` theo cách được driver hỗ trợ. Kiểm soát pool/ghi tuần tự để giảm `database is locked`. Các thiết lập theo connection phải được áp dụng nhất quán.
- Thực hiện migration có version ngay lúc startup, trong transaction khi phù hợp; backup trước migration không tương thích. Không thay đổi schema kiểu ngầm định.
- Không lưu cookie/password hay `Authorization` trong DB/log. URL có token hoặc query nhạy cảm cũng là secret; mặc định hạn chế lưu, che trong UI/log, cân nhắc keychain/credential store nếu muốn resume URL có xác thực. Không hứa xóa sạch token bằng cách chỉ xóa hàng SQLite vì WAL/backup có thể còn dữ liệu.

## 10. UI và tray

### Màn hình MVP

1. **Danh sách:** tên, trạng thái, tiến độ, tốc độ, ETA; nút thêm/tạm dừng/tiếp tục/xóa.
2. **Thêm tải:** URL, thư mục, tên file tùy chọn; báo lỗi hợp lệ trước khi tạo.
3. **Chi tiết:** kích thước, nguồn đã che, đường dẫn, số đoạn, lỗi và tùy chọn retry.
4. **Cài đặt:** thư mục mặc định, số tải đồng thời, số kết nối, chính sách khi đóng cửa sổ.

Dùng widget danh sách có tái sử dụng item thay vì tạo hàng nghìn widget; snapshot chỉ chứa dữ liệu cần hiển thị. Đo CPU/render khi nhiều job thay đổi. Snapshot tổng hợp đưa vào `fyne.Do`; không giữ mutex trong lúc chờ UI. Tốc độ dựa trên chênh lệch byte/thời gian, ETA chỉ hiện khi có tổng và tốc độ đáng tin cậy.

### Vòng đời

- Nút đóng cửa sổ: mặc định hỏi hoặc theo cài đặt «ẩn về tray»; đóng cửa sổ không phải thoát.
- Menu tray: Mở PDM, Tạm dừng tất cả, Thoát. Không gọi API Fyne tùy tiện từ worker.
- Thoát: ngừng nhận job → hủy workers → chờ với deadline → checkpoint các offset đã sync → đóng DB và file → thoát. Nếu deadline hết hoặc sync thất bại, không xác nhận trạng thái thành công giả; dựa vào checkpoint trước đó khi khởi động lại.
- Nếu tray không khả dụng trên một môi trường desktop, cung cấp cách mở/thoát rõ ràng; kiểm thử riêng từng OS. [Fyne system tray](https://docs.fyne.io/explore/systray/).

## 11. Tích hợp trình duyệt — giai đoạn sau MVP

```text
Chrome/Edge extension (JS/TS)
        │ native messaging: JSON framed over stdin/stdout
        ▼
Go native host process (do browser khởi chạy)
        │ IPC cục bộ có kiểm soát / cơ chế khởi chạy app
        ▼
PDM desktop process → validate → hộp thoại xác nhận → hàng đợi
```

- Native host là **tiến trình riêng**, không phải chính tiến trình UI. Chrome yêu cầu host manifest, `allowed_origins`, đăng ký theo OS, framing length-prefix và stdout chỉ dành cho protocol; log đưa sang stderr. Cần giới hạn kích thước message. [Chrome — Native messaging](https://developer.chrome.com/docs/extensions/develop/concepts/native-messaging).
- Extension gửi URL theo thao tác người dùng trước; không tự thu thập cookie/headers. Kiểm soát scope quyền extension tối thiểu. Firefox cần manifest/đăng ký riêng; không giả định giống Chrome.
- Nếu PDM đang chạy, native host chuyển yêu cầu qua IPC OS-local có cơ chế giới hạn quyền, xác thực peer hoặc token bảo vệ và giới hạn message. Nếu chưa chạy, thiết kế launch/handshake an toàn; không đưa URL/token nhạy cảm vào command line hoặc log.
- Từ chối scheme ngoài HTTP/HTTPS; xác nhận URL/tên đích trước khi tải, không cho extension tùy ý ghi đè path, cấu hình hoặc thực thi lệnh. Cân nhắc bảo vệ địa chỉ local/private và DNS rebinding nếu nhận URL từ nguồn không tin cậy; cho phép mạng nội bộ chỉ qua quyết định rõ ràng của người dùng. Ở MVP, quy tắc public-only đã được enforce tại UI và redirect (xem §12); browser integration sau này dùng chung quy tắc này, không tự đặt lại.
- Việc bắt/take over mọi lượt tải của browser có các giới hạn API, tải trùng và quyền riêng tư; xác minh từng trình duyệt trước khi cam kết tính năng.

## 12. Bảo mật

- Validate URL, redirects và tên file; xử lý `Content-Disposition` như dữ liệu không tin cậy. Loại path traversal, ký tự cấm, tên thiết bị Windows và đường dẫn tuyệt đối; giới hạn độ dài.
- SSRF (P1, đã triển khai): `platform.ValidatePublicHTTPURL` chỉ chấp nhận URL có host public — từ chối IP literal loopback/private/link-local/unspecified và hostname resolve ra các dải đó (hostname không resolve được cũng bị từ chối). Kiểm tra áp dụng tại UI (`add_dialog.validateURL`) và tại `CheckRedirect` của engine client. Giới hạn đã biết: (1) gọi trực tiếp `engine.Download` với URL loopback không bị chặn — engine là layer nội bộ, production chỉ service gọi nó và mọi lượt tải thật đều qua UI + redirect; (2) DNS rebinding — IP có thể đổi sau lúc resolve, chưa pin IP ở tầng dial.
- Chống ghi đè ngoài ý muốn, kể cả symlink/race; dùng primitive OS phù hợp và kiểm thử. Không chạy file tải về tự động.
- TLS bật kiểm tra chứng chỉ mặc định; không thêm tùy chọn bỏ qua TLS verification cho MVP.
- Che query string, userinfo, headers nhạy cảm trong logs, thông báo lỗi và màn hình mặc định. Không hardcode token.
- Chạy với quyền người dùng, không cần quyền admin cho lõi. Đăng ký browser host/installer có thể có yêu cầu OS riêng.
- Giới hạn số job, kết nối, thời gian chờ và bộ nhớ; không giữ toàn bộ file trong RAM.

## 13. Kiểm thử và tiêu chí hoàn thành

### Unit + integration

- Chia segment: file 0 byte, file nhỏ hơn số worker, chia dư, không overlap/gap.
- `httptest` server: 206 đúng/sai Content-Range; server trả 200 khi có Range; 416; HEAD 405; redirect; unknown length; slow/stall; drop kết nối; 429/503 Retry-After; nội dung/ETag thay đổi.
- Pause/resume tại nhiều offset; retry sau ngắt; cancel không ghi thêm sau khi hoàn tất; worker errors và cleanup. Kiểm tra output byte-for-byte bằng SHA-256 đối chiếu file fixture.
- Crash simulation: kill sau write trước sync, sau sync trước DB commit, sau commit, trước/sau rename và trước `completed`; dữ liệu không bị ghép sai và không overwrite file đích.
- SQLite migration, khôi phục DB/file thiếu hoặc không nhất quán, hết dung lượng/quyền ghi, tên trùng, invalid path.
- Dùng `go test ./...` và `go test -race ./...` trên môi trường có toolchain phù hợp; race detector không phát hiện mọi lỗi crash hay filesystem.
- Test tay Fyne/tray, DPI, accessibility, mở/đóng, sleep/wake, proxy và đường truyền chập chờn trên từng OS.

### Benchmark trước khi kết luận «nhẹ nhất»

Đo cùng thiết bị, OS, bản build release, nguồn mạng/fixture, cùng số task và tính năng:

- Dung lượng binary/installer, thời gian mở, RAM tổng của mọi process khi idle, khi ẩn tray và khi tải.
- CPU, throughput, tốc độ ghi đĩa, số kết nối, thời gian hồi UI; tải một file lớn, nhiều file nhỏ, 100/1.000 job trên danh sách.
- So sánh Go + Fyne với Flutter + Go (và Gio nếu cần); ghi rõ phương pháp và số lần chạy. Không đặt ngưỡng cứng trước khi có baseline.

## 14. Kế hoạch triển khai

| Giai đoạn | Kết quả bàn giao |
|---|---|
| 0. Bootstrap | Go module, Fyne window chạy được, CI cơ bản. |
| 1. Single stream | Thêm URL, tải một luồng vào `.part`, hiển thị và hủy an toàn. |
| 2. Queue + persistence | SQLite migrations, scheduler, restart recovery, trạng thái rõ ràng. |
| 3. Range + resume | Probe, segment, validator, checkpoint, pause/resume và test crash. |
| 4. Desktop UX | Tray, settings, packaging, lỗi dễ hiểu, kiểm thử từng OS. |
| 5. Browser | Extension, native host, IPC và phân tích bảo mật riêng. |
| 6. Tối ưu | Profile CPU/RAM/disk, benchmark; chỉ cân nhắc tách daemon khi có số liệu. |

Không cần tạo toàn bộ kiến trúc ngay từ giai đoạn 0. Tách package theo nhu cầu thực tế; tránh xây dựng plugin/event bus hay microservice cho MVP.

## 15. Khởi tạo dự án thực tế

### 15.1 Yêu cầu

- Cài Go phiên bản tương thích với phiên bản Fyne chọn, Git, C compiler và thư viện đồ họa theo hệ điều hành; xem [Fyne quick start](https://docs.fyne.io/started/quick/).
- Script dưới đây dùng **Bash** (macOS/Linux hoặc Git Bash trên Windows). Trong PowerShell, dùng Git Bash hoặc chuyển lệnh tương đương; đường dẫn và dependency OS có thể khác.
- Đường dẫn module dự kiến là `github.com/ngotuananh101/pdm`. Đây là tên đề xuất, **không khẳng định repo đã tồn tại**; đổi trước khi chạy nếu tên thực tế khác.

### 15.2 Tạo cấu trúc tối thiểu và cửa sổ chạy được

Chạy trong thư mục cha mới, tránh ghi đè dự án hiện có:

```bash
mkdir ponta-download-manager
cd ponta-download-manager

git init
go mod init github.com/ngotuananh101/pdm
mkdir -p cmd/pdm docs assets

cat > .gitignore <<'GITIGNORE'
# Build output and local app data
/bin/
/dist/
/build/
*.exe
*.test
*.out
*.db
*.db-shm
*.db-wal
*.part
.DS_Store
GITIGNORE

cat > cmd/pdm/main.go <<'GO'
package main

import (
    "fyne.io/fyne/v2"
    "fyne.io/fyne/v2/app"
    "fyne.io/fyne/v2/container"
    "fyne.io/fyne/v2/widget"
)

func main() {
    a := app.NewWithID("com.ponta.pdm")
    w := a.NewWindow("Ponta Download Manager")
    w.Resize(fyne.NewSize(900, 600))
    w.SetContent(container.NewCenter(
        widget.NewLabel("PDM — ứng dụng đang được khởi tạo"),
    ))
    w.ShowAndRun()
}
GO

cat > README.md <<'README'
# Ponta Download Manager

Desktop download manager bằng Go và Fyne.

Chạy: `go run ./cmd/pdm`
README

# Chọn bản Fyne v2 ổn định tương thích với Go, sau đó commit go.mod/go.sum.
go get fyne.io/fyne/v2@latest
go mod tidy
go run ./cmd/pdm
```

`@latest` chỉ dùng **một lần khi bootstrap**; kiểm tra phiên bản được chọn, yêu cầu Go và release notes, sau đó commit `go.mod`/`go.sum` để có build tái lập. Thay `latest` bằng phiên bản cụ thể nếu dự án cần reproducibility ngay từ đầu. Cửa sổ này chỉ xác nhận môi trường; chưa có downloader, tray hay database.

### 15.3 Sau khi cửa sổ chạy

```bash
mkdir -p internal/{app,domain,engine,scheduler,storage/sqlite/migrations,platform,ui,config}
mkdir -p docs/decisions scripts testdata .github/workflows
# Chép ARCHITECTURE.md này vào docs/ARCHITECTURE.md.
go test ./...
git add .
git commit -m "chore: bootstrap PDM desktop app"
```

Git không theo dõi thư mục rỗng; các thư mục `internal/` sẽ xuất hiện trong repo khi thêm file triển khai. Nếu `git commit` yêu cầu, cấu hình Git user name/email theo tài khoản của bạn. Không chạy `go test`/build thành công chỉ bằng script nếu môi trường chưa có các dependency Fyne phù hợp.

## 16. Quyết định cần xác nhận trước khi triển khai

Trạng thái đã xác nhận (2026-11-05). Chi tiết đầy đủ trong ADR: [`docs/decisions/001-mvp-scope-and-policies.md`](decisions/001-mvp-scope-and-policies.md).

1. **Nền tảng MVP:** Windows + Linux (đồng thời). macOS giữ portable nhưng không phải mục tiêu phát hành ban đầu. (Đã xác nhận)
2. **Mô hình tiến trình:** Ứng dụng desktop một tiến trình; đóng cửa sổ chính ẩn về tray (Giai đoạn 4) trong khi tải vẫn tiếp tục; lệnh Thoát thực hiện graceful shutdown có giới hạn thời gian. (Đã xác nhận)
3. **URL có đăng nhập/cookie trong MVP:** Không. Credential storage và browser integration đưa sang giai đoạn sau MVP. (Đã xác nhận)
4. **Tên file trùng:** Chính sách cấu hình trong Cài đặt, mặc định = hỏi người dùng (ask — chọn Tự đổi tên, Ghi đè, hoặc Hủy). Các chính sách: `ask`, `auto_rename`, `overwrite`. Dùng primitive OS atomic no-clobber (RENAME_NOREPLACE/renameat2 trên Linux, MoveFileExW không MOVEFILE_REPLACE_EXISTING trên Windows), không dựa vào Stat+Rename. (Đã xác nhận)
5. **Browser integration:** Không bắt buộc ở bản đầu; đưa sang Giai đoạn 5. (Đã xác nhận)

