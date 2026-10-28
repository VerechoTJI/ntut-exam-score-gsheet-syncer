# 助教操作手冊：成績試算表與資料庫備份

> 文字初稿。本文先提供操作步驟、指令與驗收方式；「預留圖」標記後續要補的截圖及圖說。

本手冊供負責考試成績與備份的助教使用，適用於 `ntut-exam-score-gsheet-syncer`。你可以用它建立成績試算表、同步各教室的評測結果，以及把教師機的 SQLite 資料庫備份到 Google Drive。

## 1. 流程與目錄

### 1.1 考前準備流程

考前準備可在後端尚未啟動時完成。Google 授權、設定分享權限與建表需要連線到 Google；後端網址與管理員 token 可先取得並填入 `.env`。

| 操作 | 相關段落 | 完成後得到什麼 |
| --- | --- | --- |
| 準備電腦、工具及本次考試檔案 | 第 2 節 | 可執行程式的部署資料夾、名冊與 exam config |
| 確認既有 credentials 並完成登入 | 第 3.1 節確認憑證；第 3.2 節接收檔案與登入 | 可使用的 Sheets 憑證，以及備份需要的個人 Drive 授權 |
| 建立 Sheet 與備份資料夾 | 第 4 節 | 已設定存取權的試算表與備份目的地 |
| 從名冊與設定建立完整試算表 | 第 5 節 | 五個分頁、名冊、計分公式、統計與圖表 |
| 填寫本教室的連線與間隔設定 | 第 6.1 節；只備份時看第 8.1 節 | 已填好本教室資訊的 `.env` |
| 規劃多個教室或準備下一場考試 | 第 11 節 | 各教室的獨立部署資料夾與對應設定 |

### 1.2 考試時部署流程

完成所需的考前準備、啟動本教室後端後，依要使用的功能選擇流程，先 dry-run 預覽，再持續執行。此階段確認後端連線與管理員 token、預覽成績更新，並以成功上傳的資料庫快照驗證實際備份。

| 操作 | 相關段落 | 完成後得到什麼 |
| --- | --- | --- |
| 同步成績並定期備份 | 第 6.2～6.3 節 | 定期同步成績並備份 |
| 只同步成績 | 第 7 節 | 只定期同步本教室的成績試算表分頁 |
| 只備份資料庫 | 第 8 節 | 只定期上傳本教室的 `.sqlite` 備份 |
| 查看成績、處理異常 | 第 9 節、第 12 節 | 確認資料來源與處理方式 |
| 停止、重新啟動或考後收尾 | 第 10 節 | 完成最後同步與備份，保存交接資料 |

**操作原則：同一份試算表建表一次；每個教室各自執行同步。** `--setup-sheet` 會重建目標分頁；日常更新成績使用第 6 或第 7 節的同步流程。

**預留圖 01｜整體資料流。** 畫出「學生端 → 教師機後端 → 同步器」，再分支到「Google Sheets」及「Google Drive 備份資料夾」，標明各處負責保存的資料。

## 2. 共通準備：資料夾、工具與檔案

### 2.1 準備執行環境

準備一台可連線到 Google 的部署電腦；考試時部署另需能連線到本教室後端。持續同步時，電腦需保持開機、連線，執行程式的終端機也要保持開啟。

1. 取得本專案資料夾 `ntut-exam-score-gsheet-syncer`。
2. 安裝 Python 3.13 或更新版本，以及 [uv](https://docs.astral.sh/uv/getting-started/installation/)。
3. 在含有 `main.py` 的資料夾開啟 PowerShell 或 Bash。
4. 安裝鎖定版本的套件。

PowerShell：

```powershell
uv sync --frozen
```

Bash：

```bash
uv sync --frozen
```

完成檢查：指令成功結束，資料夾中產生 `.venv`。後續每次執行指令，都從此資料夾開始；本文的檔案路徑皆相對於此處。

### 2.2 取得本次考試資料

| 檔案或資訊 | 向誰取得／如何準備 | 使用時機 |
| --- | --- | --- |
| `credentials.json` | 向負責保管既有 Service account 憑證的人員取得，依第 3.1 節維護 | 建表、成績同步 |
| `drive_oauth_client.json` | 管理者依第 3.1 節建立 Desktop OAuth client 並交付 | 首次 Drive 授權或重新授權 |
| `drive_token.json` | 助教依第 3.2 節登入自己的帳號後在本機產生 | 備份期間自動更新認證 |
| `roster.xlsx` | 向課程負責人取得 Excel 名冊 | 建表 |
| `exam-config.json` | 向考試系統管理者取得本次完整考試設定 | 建表、成績同步 |
| 後端網址、管理員 token | 向本教室教師機管理者取得 | 成績同步、資料庫備份 |
| Sheet 網址、Drive 資料夾網址 | 第 4 節建立 | 指定成績與備份目的地 |

全部功能的部署資料夾會像這樣：

```text
ntut-exam-score-gsheet-syncer/
  main.py
  uv.lock
  .env.template
  .env
  credentials.json
  drive_oauth_client.json
  drive_token.json
  roster.xlsx
  exam-config.json
```

只用部分功能時，依上表準備對應檔案即可。憑證、token、名冊及考題設定請透過受控管道交接，避免放入公開文件、截圖或 Git。

**預留圖 02｜部署資料夾。** 使用示範檔案顯示檔名與副檔名；另外標出 `main.py` 與終端機目前所在位置。

## 3. Google 設定：管理者準備，助教登入

Google 存取分成兩個身分：Service account 寫入 Sheet；助教授權的 Google 帳號上傳 My Drive 備份。管理者統一提供應用程式與服務帳戶的 JSON；每位負責備份的助教在自己的部署環境登入，產生個人的授權 token。

### 3.1 維護既有 credentials 與準備 Drive OAuth

本專案的 Service account 已建立。日常部署沿用既有帳戶的 `credentials.json`，由負責保管該憑證的人員提供可用版本；已取得檔案的助教確認下列事項後，接續第 3.2 節操作。

**Sheets 憑證 `credentials.json`：**

1. 將收到的既有金鑰檔保存為部署資料夾中的 `credentials.json`，限制檔案存取並透過受控管道交接。
2. 核對 JSON 的 `client_email` 是否為本專案既有 Service account，保留此 email 供第 4.1 節設定 Sheet 編輯權限。
3. 完成第 4.1 節的 Sheet 分享後，考前依第 5.2 節執行建表 dry-run，確認憑證與試算表存取正常。
4. 需要更新金鑰檔時，先停止使用該檔案的同步程序，向憑證保管人取得更新版本，替換 `credentials.json`。核對更新後的 `client_email`；若帳戶有變更，先調整 Sheet 分享對象。考前可用第 5.2 節建表 dry-run 檢查存取；考試時部署則依第 6.2 或 7.2 節完成分數預覽，再恢復持續同步。

完成檢查：部署資料夾有可用的 `credentials.json`，Service account email 正確，且可讀取目標 Sheet。首次建立或重建 Service account 的背景流程收在[附錄 A](#appendix-a)。

**Drive 應用程式憑證 `drive_oauth_client.json`：**

1. 在 Google Cloud 專案的 API 程式庫啟用 **Google Drive API**。只使用備份功能時，也要完成此步。
2. 開啟 Google Auth Platform／OAuth 同意畫面，設定應用程式資訊與 Audience。
3. 若供學校組織內助教使用，將專案歸屬該 Google Workspace／Cloud Identity 組織，Audience 選 **Internal**。同組織帳號可授權，並由組織管理員確認允許應用程式存取所需 Drive 權限。[Google Audience 說明](https://support.google.com/cloud/answer/15549945?hl=en)
4. 建立類型為 **Desktop app** 的 OAuth client，下載並命名為 `drive_oauth_client.json`。
5. 將此檔案安全交付給需要備份的助教，放在各自部署資料夾；同一應用程式的 client JSON 可供多位助教使用，每位助教各自完成第 3.2 節登入。

Internal 的組織內使用者可直接授權，無須逐一加入 Test users。若部署需使用組織外帳號，管理者改依 External 流程處理；External／Testing 須加入 Test users，這類 Drive 授權的 refresh token 通常七天後失效。Internal 依組織政策管理，授權仍可能因撤銷或政策變更而失效。[Google OAuth 狀態說明](https://developers.google.com/identity/protocols/oauth2/production-readiness/overview)、[token 有效期說明](https://developers.google.com/identity/protocols/oauth2#expiration)

完成交付：依啟用功能提供 `credentials.json`、Service account email、`drive_oauth_client.json`，並告知助教應使用的組織帳號範圍。Drive 的個人 `drive_token.json` 由助教登入後產生，保留在對應部署環境。

**預留圖 03｜既有 credentials 核對。** 顯示部署資料夾中的檔名與 `client_email` 的位置；示意 email 使用假資料，遮蔽私鑰全文。

### 3.2 助教：接收檔案並登入自己的帳號

準備：已取得管理者提供的 JSON，部署電腦可開啟瀏覽器。助教使用自己的組織帳號，並確認該帳號有備份資料夾的新增檔案權限。

1. 建表或同步成績時，將管理者提供的 `credentials.json` 放在部署資料夾，取得對應的 Service account email，再依第 4.1 節設定 Sheet 分享。只用 Sheets 功能時，完成此項即可接續操作。
2. 使用備份功能時，將管理者提供的 `drive_oauth_client.json` 放在部署資料夾。
3. 執行以下授權指令。

PowerShell：

```powershell
uv run python main.py --authorize-drive --oauth-client "drive_oauth_client.json"
```

Bash：

```bash
uv run python main.py --authorize-drive --oauth-client "drive_oauth_client.json"
```

4. 在開啟的瀏覽器選擇自己的組織帳號，核對管理者提供的應用程式與要求的 Drive 權限，完成同意。Internal 應用程式需選專案所屬組織內的帳號。
5. 回到終端機，確認出現 `Google Drive authorization saved locally.`，且資料夾產生 `drive_token.json`。

程式之後使用此助教的 token 自動更新認證，並以該帳號上傳備份。此版本使用完整 Drive 存取範圍，請依組織核准的方式授權；遇到組織封鎖提示時，將錯誤交給管理者確認政策。過期、撤銷或更換負責帳號時，重新執行本節指令。

考前完成檢查：有 `drive_token.json`，並已記下登入的是哪個帳號。實際資料庫下載與上傳驗證安排在考試時部署，依第 6.3 或 8.3 節核對成功上傳。

**預留圖 04｜管理者設定與助教登入。** 分成兩組：管理者的 Internal Audience／Desktop app 畫面，以及助教的組織帳號選擇／終端機成功訊息；遮蔽 client secret 與 token。

## 4. 建立成績與備份目的地

### 4.1 建立本次考試的 Google Sheet

1. 使用考試管理帳號建立一份空白試算表，以課程與考試名稱命名。
2. 點選「共用」，將一般存取權設為 **知道連結的任何人 → 檢視者**。
3. 另外加入第 3.1 節的 `client_email`，授予 **編輯者**。
4. 複製試算表網址，保留供建表與 `.env` 設定使用。

公開檢視會讓持有連結的人看到名冊與成績；若課程採限定分享，改成「受限制」並加入獲准查看的人。兩種方式都要另外保留 Service account 的編輯權。[Google 分享權限說明](https://support.google.com/drive/answer/2494822)

完成檢查：試算表的共用名單內，Service account email 旁顯示「編輯者」。

**預留圖 05｜試算表共用視窗。** 同時標出一般存取權的檢視者及 Service account 的編輯者，讓助教能分別核對。

### 4.2 建立 Drive 備份資料夾

1. 用第 3.2 節授權的帳號開啟 My Drive。
2. 建立本次考試的備份資料夾，例如「程式設計期末考 DB 備份」。也可使用該帳號有新增檔案權限的既有資料夾。
3. 保持備份資料夾為受限制存取，依交接需要授權管理人員。
4. 開啟資料夾，複製網址。

以下兩種網址都可使用，`FOLDER_ID` 換成實際 ID：

```text
https://drive.google.com/drive/folders/FOLDER_ID
https://drive.google.com/drive/u/5/folders/FOLDER_ID
```

`/u/5/` 是瀏覽器帳號切換路徑；程式上傳時使用 `drive_token.json` 對應的帳號。完成檢查：授權帳號可開啟資料夾並新增檔案，且已複製正確網址。

**預留圖 06｜備份資料夾。** 標出資料夾名稱、網址與目前 Google 帳號，使用空白示範資料夾。

## 5. 功能一：從名冊與設定建立完整試算表

用途：在考前一次建立各教室輸入分頁、彙總、最終計分、統計與折線圖。

### 5.1 準備與核對檔案

管理者先完成第 3.1 節的 Sheets 憑證準備；助教完成第 2 節、3.2 節第 1 項與 4.1 節，再準備：

- `roster.xlsx`：選定分頁的第一列含 `班級`、`姓名`、`學號`，欄位順序可不同。每位學生需有唯一且完整的學號；核對前導零是否已在原始 Excel 中正確保存。
- `exam-config.json`：本次考試完整設定，包含 `sections[].puzzles[]`、子題配分、規則倍率及分區上限。向考試管理者取得與本次後端／學生端對應的版本。
- 名冊分頁名稱：以下示範 `工作表1`，執行前換成 Excel 下方顯示的名稱。

建表與成績同步統一指定本次 `exam-config.json`，依同一份設定決定題目順序、子題對應與計分。

**預留圖 07｜示範名冊。** 使用三位虛構學生，框出第一列三個必要欄位與底部工作表名稱。

### 5.2 預覽目的地與現有內容

把 `TARGET_ID` 換成第 4.1 節的試算表 ID；也可以將整段示範網址換成實際完整網址。

PowerShell：

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --config "exam-config.json" --roster "roster.xlsx" `
  --roster-sheet "工作表1" --dry-run
```

Bash：

```bash
uv run python main.py --setup-sheet \
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" \
  --config "exam-config.json" --roster "roster.xlsx" \
  --roster-sheet "工作表1" --dry-run
```

確認輸出的試算表名稱、網址、學生列數，以及現有資料／公式／圖表數量。此步會讀取線上試算表，保留現有內容。

### 5.3 執行建表

PowerShell：

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --config "exam-config.json" --roster "roster.xlsx" `
  --roster-sheet "工作表1"
```

Bash：

```bash
uv run python main.py --setup-sheet \
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" \
  --config "exam-config.json" --roster "roster.xlsx" \
  --roster-sheet "工作表1"
```

若目標分頁已有資料、公式或圖表，程式會顯示摘要與覆寫警告，詢問 `Continue? [y/N]`。核對目的地並確認要重建後，輸入 `y` 或 `yes`；輸入 `n` 或直接 Enter 可取消。空白目的地會直接建表。

重新建表會重設五個目標分頁的內容、格式與圖表。對已有成績的試算表，先停止所有教室同步、保留副本，再評估是否重建；重建後的來源分頁需重新同步。

### 5.4 完成檢查

終端機應出現 `Setup complete`。開啟 Sheet，逐項確認：

| 分頁 | 內容及檢查重點 |
| --- | --- |
| `1222`、`1223`、`335` | 三個固定來源分頁；學號清單、題目與子題數符合本次考試 |
| `Combined` | 依學號加總三個教室的子題／規則旗標，跨頁公式存在 |
| `成績登記` | 班級、姓名、學號、配分、規則與分區計分，最後成績及原始分數 |
| `成績登記` 右側 | 及格／不及格、分數區間、累計人數，以及「分數區段人數統計和累計人數」雙數列折線圖 |

抽查首位與末位學生、題目配分及分區上限，確認公式無 `#REF!` 等錯誤。後續只更新各教室來源分頁，彙總與計分由公式連動。

**預留圖 08｜建表預覽與覆寫確認。** 以測試表呈現摘要、警告及取消／繼續的輸入位置。

**預留圖 09｜建表成果。** 分別截來源分頁表頭、Combined 公式、成績登記表頭及右側折線圖；使用合成名冊與成績。

## 6. 功能二：持續同步成績並定期備份

用途：考試期間在同一程序中更新本教室分頁，並每 300 秒備份資料庫。

第 6.1 節的設定檔可在考前填寫。進入第 6.2～6.3 節的預覽與啟動時，需完成第 2～5 節，並確認本教室後端可連線、管理員 token 已取得、後端已有 scoreboard 資料。API 回傳空清單時，目前分數同步會回報缺少必要欄位並退出，可先單獨部署備份。

### 6.1 建立 .env

本節只準備設定檔，填寫時可先保留尚未啟動的後端網址與已取得的 token；連線驗證在下一節進行。

首次建立使用以下指令；已存在的 `.env` 會保留，接著用文字編輯器修改。

PowerShell：

```powershell
if (-not (Test-Path -LiteralPath ".env")) {
  Copy-Item .env.template .env
}
```

Bash：

```bash
if [ ! -f .env ]; then
  cp .env.template .env
fi
```

以下是 `1222` 教室範例。把兩個網址、後端位址及管理員 token 換成實際值。

```dotenv
# 成績目的地：本次考試的 Sheet 與本教室來源分頁
SHEET_URL=https://docs.google.com/spreadsheets/d/TARGET_ID/edit
SHEET_TITLE=1222
# 分數同步間隔，單位秒
INTERVAL=60

# 本教室後端根網址與管理員 token
BACKEND_API_URL=http://localhost:3000
ADMIN_TOKEN=replace-with-the-backend-admin-token

# Drive 備份目的地、備份間隔（秒）、檔名的教室名稱
BACKUP_FOLDER=https://drive.google.com/drive/u/5/folders/FOLDER_ID
BACKUP_INTERVAL=300
CLASSROOM=1222

# 後端下次退避等待的停止門檻（秒），預設 10
BACKOFF_MAX_SECONDS=10
# Google 每次連線／讀取等待的 timeout（秒），預設 30
GOOGLE_TIMEOUT_SECONDS=30
```

| 變數名稱 | 簡單說明 |
| --- | --- |
| `SHEET_URL` | 分數寫入的 Google 試算表網址 |
| `SHEET_TITLE` | 本教室來源分頁：`1222`、`1223` 或 `335` |
| `INTERVAL` | 分數同步間隔，填正數；本文使用 60 秒 |
| `BACKEND_API_URL` | 本教室後端根網址，例如 `http://192.168.1.10:3000` |
| `ADMIN_TOKEN` | 後端管理員 token，用於讀取分數與下載資料庫 |
| `BACKUP_FOLDER` | Drive 資料夾網址或 ID；留空停用備份 |
| `BACKUP_INTERVAL` | 備份間隔，填正數；本文使用 300 秒 |
| `CLASSROOM` | 備份檔名中的教室名稱；留空沿用 `SHEET_TITLE` |
| `BACKOFF_MAX_SECONDS` | 後端重試時，下次等待超過此值就停止；預設 10 秒 |
| `GOOGLE_TIMEOUT_SECONDS` | Google 每次連線／讀取 timeout；預設 30 秒 |

後端網址填根網址，API 路徑由程式加入。`SHEET_TITLE` 指向來源分頁；`Combined`、`成績登記` 由公式計算。每次修改 `.env` 後重新啟動程序。若 shell 已設定同名環境變數，該值會優先於 `.env`，排錯時也要核對啟動環境。

**預留圖 10｜.env 編輯範例。** 標示需要替換的欄位，用假 token；在 Sheet、教室後端、Drive 目的地之間畫出對應關係。

### 6.2 Dry-run 預覽

考試時部署先啟動本教室後端，確認後端網址與管理員 token 對應，再執行以下分數預覽。

PowerShell：

```powershell
uv run python main.py --config "exam-config.json" --dry-run
```

Bash：

```bash
uv run python main.py --config "exam-config.json" --dry-run
```

本節的 `.env` 已設定 `BACKUP_FOLDER`，所以預覽會完成一輪後結束。檢查 `Dry-run: would update` 列出的格子與值，並留意學生／題目的警告。`Rows updated: 0` 是預覽實際寫入數，預計變更請看前面的逐格輸出。備份預覽只列出計畫；實際下載與上傳權限在下一步持續執行時確認。

### 6.3 持續執行並驗收

PowerShell：

```powershell
uv run python main.py --config "exam-config.json"
```

Bash：

```bash
uv run python main.py --config "exam-config.json"
```

啟動後立即同步與備份，之後各自按間隔執行。兩種工作依序處理；例如備份正在進行時，分數同步會等它完成。各工作以上次完成時間起算下一次等待時間。

完成檢查：

1. 終端機出現 `Batch-updated ... cells across ... rows`，或符合預期的 `No rows needed updating`。
2. 抽查已知評測結果的學生，來源分頁的子題／規則旗標與後端資料一致。
3. `Combined`、`成績登記` 與圖表隨來源資料更新。
4. Drive 出現新備份，等待超過一個備份週期後再確認有後續檔案。
5. 持續留意終端機；API 或上傳失敗時程序可能退出，依第 12 節排錯後重啟。

**預留圖 11｜成功執行。** 左側放終端機同步／上傳訊息，右側放對應的測試 Sheet 與 Drive 檔案，標明三處核對位置。

## 7. 功能三：只同步成績

前置條件：管理者已依第 3.1 節交付 Sheets 憑證；助教完成第 2 節、3.2 節第 1 項、4.1 節及第 5 節建表，取得後端網址與管理員 token。已有合規試算表時先核對名冊、前兩列表頭及本次題目配置。

### 7.1 準備設定

依第 6.1 節建立 `.env`，填入 `SHEET_URL`、`SHEET_TITLE`、`BACKEND_API_URL`、`ADMIN_TOKEN`，並將以下兩項設為：

```dotenv
INTERVAL=60
BACKUP_FOLDER=
```

此流程使用 Service account 存取 Sheet，備份停用。準備與建表相同的 `exam-config.json`，後續每次預覽及啟動都指定這份設定。

### 7.2 Dry-run 預覽

PowerShell：

```powershell
uv run python main.py --config "exam-config.json" --dry-run
```

Bash：

```bash
uv run python main.py --config "exam-config.json" --dry-run
```

只同步模式下，dry-run 也會依 `INTERVAL=60` 持續預覽。檢查 `Dry-run: would update` 的格子、題目對應與警告，確認後按 Ctrl+C，等 shell 提示字元回來，再執行下一步。

### 7.3 持續同步

PowerShell：

```powershell
uv run python main.py --config "exam-config.json"
```

Bash：

```bash
uv run python main.py --config "exam-config.json"
```

啟動時立即同步，之後每次完成後等待 60 秒再同步。抽查來源分頁與後端資料，以及彙總、成績和圖表的連動。程式只寫入有變化的格子，已有相同數值時可顯示無須更新。確認第一輪與後續更新正常；需要停止時按 Ctrl+C。

## 8. 功能四：只備份資料庫

用途：持續備份本教室後端，適合尚未建表或備份與成績同步分開部署時使用。

### 8.1 準備設定

完成第 2 節、3.2 節與 4.2 節；依第 6.1 節的方式建立 `.env`，填入：

```dotenv
BACKEND_API_URL=http://localhost:3000
ADMIN_TOKEN=replace-with-the-backend-admin-token
BACKUP_FOLDER=https://drive.google.com/drive/folders/FOLDER_ID
BACKUP_INTERVAL=300
CLASSROOM=1222
BACKOFF_MAX_SECONDS=10
GOOGLE_TIMEOUT_SECONDS=30
```

換成實際後端、管理員 token、資料夾及教室。此流程使用管理員備份 API 及 Drive OAuth，所需 Google 檔案為 `drive_token.json`；重新授權時另用 `drive_oauth_client.json`。可以在尚未建立成績 Sheet 時執行。

### 8.2 Dry-run 預覽

PowerShell：

```powershell
uv run python main.py --backup-only --dry-run
```

Bash：

```bash
uv run python main.py --backup-only --dry-run
```

完成檢查：顯示預計檔名與資料夾 ID，完成預覽後退出。此模式略過後端下載及 Google 存取驗證；實際可用性以第 8.3 節持續備份的成功上傳為準。

### 8.3 持續備份

考試時部署確認本教室後端已啟動、`.env` 的後端網址與管理員 token 正確，且 `BACKUP_INTERVAL=300`，執行：

PowerShell：

```powershell
uv run python main.py --backup-only
```

Bash：

```bash
uv run python main.py --backup-only
```

啟動時立即備份，之後每次完成後等待 300 秒。完成檢查：終端機顯示 `Uploaded` 與檔案網址，指定資料夾中有新檔案；接著確認後續週期也成功上傳。按 Ctrl+C 停止；若同時有第 6 節的完整程序，先分配好備份責任，避免重複執行相同教室備份。

檔名格式為 `classroom_timestamp.sqlite`，例如：

```text
1222_20261028T143000123456+0800.sqlite
```

時間採 Asia/Taipei，含微秒與時區。檔案是後端傳回的 SQLite 二進位快照，程式驗證後原樣上傳；下載暫存檔會在此輪結束時清理，需保存的成果在 Drive。

## 9. 查看分數、統計與異常

### 9.1 各分頁如何使用

教室分頁保存各學生的子題與規則旗標，子題通過通常寫入 `1`，未通過寫入空白。`Combined` 依學號將三個教室結果相加；學生順序在各頁不同時，仍以學號對應。

`成績登記` 依 config 套用子題配分、每題一次的規則倍率與分區上限，最後四捨五入。原始分數供比較未套規則扣分及分區上限的結果。右側統計與折線圖連動呈現分數分布及累計人數。

維持建表產生的表頭、跨頁公式與圖表資料範圍。需要人工紀錄時，使用獨立分頁，讓同步欄位持續由程式管理。

### 9.2 Combined 出現大於 1

同一位學生原則上只在一間教室應試。SUM 保留跨教室的重複結果，數值大於 1 會標色，供調查重複應試或學號輸入問題。

1. 記下學生、題目、子題或規則欄位。
2. 回查三間教室對應學號及後端提交紀錄，核對實際應試位置。
3. 與監考／授課者確認處理方式，先保留原始紀錄，再修正來源資料。
4. 重新同步相關教室並核對彙總與成績。若在來源 Sheet 人工修正，須一併處理後端資料，後續同步仍會依後端內容更新。

### 9.3 某題出現系統錯誤警告

程式遇到 `SE`、`CTLE`、`ABORTED`、系統歸因錯誤，或特殊規則引擎的 `ruleId: "__engine_error__"`，會保留該學生該題的現有儲存格，印出警告原因，繼續處理其他題目與學生。

記下學生、題目與完整原因，交由負責評測環境的人排查。修復並由評測端產生有效結果後，重新同步，再核對該題。排查期間，Sheet 上保留的值需視為先前資料。

**預留圖 12｜異常判讀。** 用合成資料展示 Combined 的 `2` 標色，以及一則只跳過單一學生單一題目的系統錯誤警告。

## 10. 停止、重啟與考後收尾

### 10.1 停止或修改設定

1. 在執行中的終端機按 Ctrl+C，等 shell 提示字元回來。
2. 修改 `.env` 或替換經確認的設定檔。
3. 依原來選擇的第 6、7 或 8 節重啟；同步設定變更後先做預覽。
4. 重新確認第一輪結果與後續週期。

程序需持續執行才能輪詢；關閉終端機、電腦睡眠或重開機後，請依原流程重新啟動。本工具以終端機執行，背景服務管理需另行部署。

### 10.2 考後完成最後同步與備份

1. 與監考者確認學生端最後結果已上傳到各教室後端。
2. 保持各教室同步程序執行，確認最後結果到達後，後續一輪同步已成功完成；依第 9 節抽查來源資料與成績。
3. 保持備份程序執行，確認最後結果到達後，已有新快照成功上傳；核對檔案時間與教室。原先停用備份時，依第 8 節預覽並啟動持續備份，完成核對。
4. 確認各教室最後同步與備份皆完成後，在對應終端機按 Ctrl+C，等 shell 提示字元回來。
5. 在 `成績登記` 檢查名冊、異常值、系統錯誤待處理題目、總分及圖表，依課程規定保存試算表副本或匯出成績。
6. 交接 Sheet 連結、備份資料夾連結、各教室最後成功時間及待處理事項；憑證與 token 使用受控管道交接。

備份的還原屬教師機資料復原工作；本工具提供下載與上傳快照，需還原時交由後端管理者依教師機復原流程處理。

## 11. 多教室與下一場考試

### 11.1 三個教室共用一份成績表

先由一位負責人對共用 Sheet 執行一次第 5 節建表。每間教室準備獨立部署資料夾及 `.env`：

| 設定 | 1222 教室 | 1223 教室 | 335 教室 |
| --- | --- | --- | --- |
| `SHEET_URL` | 同一份考試 Sheet | 同一份考試 Sheet | 同一份考試 Sheet |
| `SHEET_TITLE` | `1222` | `1223` | `335` |
| `CLASSROOM` | `1222` | `1223` | `335` |
| 後端網址與管理員 token | 1222 後端對應值 | 1223 後端對應值 | 335 後端對應值 |
| `BACKUP_FOLDER` | 可用同一考試資料夾 | 可用同一考試資料夾 | 可用同一考試資料夾 |

各部署使用同一版本的完整考試 config，並分別確認 Google 權限與備份授權。即使部署在同一台電腦，也使用各自的資料夾與終端機。每個來源分頁只安排一個寫入程序；考試時部署、各教室後端啟動後，依第 6.2～6.3 節預覽與啟動，再各自驗收同步與備份。

### 11.2 換下一場考試

1. 完成第 10.2 節，保留上一場考試 Sheet 與備份。
2. 準備新考試名冊與完整 config，核對題目順序與配分。
3. 依第 4 節建立新 Sheet 與本次備份資料夾。
4. 依第 5 節對新 Sheet 建表。
5. 更新各教室 `.env` 的新連結、後端資訊及教室，確認 Drive 授權仍有效。
6. 考試時部署、各教室後端啟動後，依選用功能執行第 6、7 或 8 節的預覽／驗證，再開始常駐。

## 12. 常見問題與處理

| 現象 | 檢查與處理 | 恢復後的確認 |
| --- | --- | --- |
| 找不到 `main.py`、JSON 或 Excel | 回到含 `main.py` 的部署資料夾，核對檔名、副檔名及相對路徑 | 重新執行原功能的預覽 |
| Sheet 403、Permission denied、找不到試算表 | 核對 `SHEET_URL`、Service account `client_email` 的編輯權及兩個 Google API 是否啟用 | 建表／同步預覽成功，接著驗證實際操作 |
| `WorksheetNotFound` | 核對 `SHEET_TITLE` 與 Sheet 下方來源分頁名稱；需要建表時先依第 5 節處理 | 同步預覽對應到正確教室 |
| 後端 401／403 | 核對本教室後端根網址及管理員 token | 重新預覽，並確認持續同步或備份成功 |
| 連線失敗、timeout、重試後退出 | 依輸出的具體原因檢查教師機、網路及服務狀態，再重啟程序 | 確認第一輪與下一輪成功 |
| `Missing required columns`，API 尚無 scoreboard 列 | 確認後端初始化與 scoreboard 資料；備份可先獨立執行 | 有資料後重新做分數預覽 |
| config／題號／子題配置錯誤 | 核對是否使用本次完整 config、實際 puzzle ID 與 Sheet 表頭；保留現有成績再決定修正方式 | 預覽通過，抽查題目對應 |
| 名冊缺欄位、缺學號、重複學號或分頁歧義 | 修正 Excel 第一列與學生資料；以 `--roster-sheet` 指定分頁 | 建表預覽學生數正確 |
| Drive token 過期、撤銷或缺少 | 依第 3.2 節重新授權正確備份帳號 | 第 8.3 節持續備份上傳成功 |
| Drive 資料夾權限錯誤 | 核對 `BACKUP_FOLDER` 及 OAuth 帳號的新增檔案權限 | 指定資料夾出現新備份 |
| SQLite 驗證或上傳驗證失敗 | 保留錯誤原因，檢查後端快照與雲端檔案，交由管理者處理後再備份 | 新一輪驗證並上傳成功 |
| `No rows needed updating` | 抽查後端是否有新結果、是否選對教室，以及 Sheet 是否已相同 | 已知學生的旗標與結果一致 |
| 只同步模式的 `--dry-run` 持續輸出 | 依第 7.2 節檢查預覽，確認後按 Ctrl+C 回到 shell | 啟動第 7.3 節的持續同步，核對實際更新 |

### 12.1 如何理解重試與 timeout

`BACKOFF_MAX_SECONDS` 控制後端 API 失敗後，下一次退避等待的停止門檻。預設 10 秒時，失敗會依序等待 1、2、4、8 秒；若又失敗，下一次需等 16 秒，程式就報錯停止。每次請求另有 10 秒的連線／讀取等待設定，因此多次嘗試與等待加起來可能超過 10 秒。

`GOOGLE_TIMEOUT_SECONDS` 控制 Google HTTP 請求每次連線／讀取的等待，預設 30 秒。Google 認證更新交由 SDK 處理；瀏覽器登入由助教完成，登入操作時間另計。這兩個設定都接受大於 0 的有限數字，空白時採預設。

### 12.2 自訂憑證位置

如有自訂路徑需求，`--sheet-url` 可覆蓋 `.env` 的 Sheet 網址，`--backup-folder`／`--backup-interval`／`--classroom` 可覆蓋備份設定。`--oauth-client` 與 `--oauth-token` 可指定相對路徑，授權與備份時須使用相同 token 位置；Sheets 金鑰固定依部署資料夾的 `credentials.json` 載入。

## 13. 插圖製作與文件交接

後續依圖 01～12 補圖，每張圖搭配「操作位置、要核對的值、成功畫面」圖說。教學圖統一使用合成名冊、示範題目與測試 Sheet；截圖前移除真實學號、成績、私鑰及 token。

首次交付前，由另一位助教使用測試環境，依選用功能完整走一次，記錄實際 Google 畫面名稱差異、操作卡點與驗收結果，再整理成圖文版。本文指令依目前 CLI 與 [README](../README.md) 整理；Google 實際授權與寫入結果需由操作當下的驗收確認。

<a id="appendix-a"></a>

## 附錄 A：首次建立 Service account

本附錄供具有 Google Cloud 專案管理權限的人員，在首次建立或另行規劃重建服務帳戶時參考。日常部署依第 3.1 節維護既有 `credentials.json` 即可。建立憑證的畫面可對照 [Google 官方說明](https://developers.google.com/workspace/guides/create-credentials)。

1. 開啟 [Google Cloud Console](https://console.cloud.google.com/)，選擇或建立供考試使用的專案。
2. 在 API 程式庫啟用 **Google Sheets API** 與 **Google Drive API**。
3. 到「IAM 與管理員 → 服務帳戶」建立 Service account。
4. 在該服務帳戶的「金鑰」頁建立 JSON 金鑰並下載。
5. 將檔案命名為 `credentials.json`，放到部署資料夾。
6. 記下 JSON 的 `client_email`，連同 `credentials.json` 安全交付給負責部署的助教。
7. 依第 4.1 節把目標 Sheet 分享給此 email，授予編輯者，再依第 5.2 節執行建表 dry-run 驗證 Google 存取。
