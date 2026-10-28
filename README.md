# 國立臺北科技大學考試成績同步與備份工具

將 `ntut-exam-v2` 後端的學生端評測結果同步到 Google 試算表，並定期把 SQLite 資料庫備份到 Google Drive。評測與最高分選擇仍由上機測驗學生端負責；本程式只負責同步、建表與備份。

助教依功能操作的完整流程，請見[助教操作手冊（文字初稿）](docs/ta-operation-guide.md)，包含建表、同步、備份、考後收尾及常見問題處理。

## 部署流程

依序完成：**準備憑證 → 確認共享根目錄權限與準備連結 → 放入名冊／設定並初始化 → 填寫 `.env`、驗證及啟動**。

考前可完成憑證、Google 授權、建表與 `.env` 填寫，這些步驟只需本機檔案及 Google 連線。考試時部署啟動後端後，再預覽成績同步、持續同步並驗證實際備份。

先將本專案放到部署機，安裝 Python 3.13+ 與 [uv](https://docs.astral.sh/uv/getting-started/installation/)，並在 **本專案使用說明所在資料夾**開啟終端機。下列檔案路徑都相對於這個資料夾；每個指令都有 PowerShell 與 Bash 版本。

### 1. 準備憑證檔案與 Google Drive 登入授權

先安裝專案相依套件。

PowerShell：

```powershell
uv sync --frozen
```

Bash：

```bash
uv sync --frozen
```

助教向維護考試系統的同學取得以下檔案，放進程式資料夾：

- `credentials.json`：讓程式讀寫成績試算表，沿用現有檔案即可。
- `drive_oauth_client.json`：供 Google Drive 登入使用，沿用現有檔案即可。

需要備份資料庫時，助教執行登入指令，用自己的學校 Google 帳號授權。完成後，程式會自動產生 `drive_token.json`，用於後續備份。

| 用途 | Google 身分 | 檔案 |
| --- | --- | --- |
| 建立工作表內容、同步分數 | Google 服務帳戶 | `credentials.json` |
| 上傳備份到 Google Drive 的「我的雲端硬碟」 | 你的 Google 帳號，完成 Google 帳號授權後登入 | `drive_oauth_client.json`，登入後另產生 `drive_token.json` |

**助教操作：確認檔案並登入。**

1. 確認 `credentials.json` 與 `drive_oauth_client.json` 在本專案資料夾。
2. 執行以下指令，在開啟的瀏覽器登入具有共享根目錄編輯權的學校 Google 帳號，並完成授權。Internal 應用程式需選專案所屬組織內的帳號。

PowerShell：

```powershell
uv run python main.py --authorize-drive --oauth-client "drive_oauth_client.json"
```

Bash：

```bash
uv run python main.py --authorize-drive --oauth-client "drive_oauth_client.json"
```

成功後會在本機產生 `drive_token.json`，讓之後的備份自動更新權杖；排程執行期間不會自行跳出瀏覽器要求登入。預設使用完整 Google Drive 存取範圍，以支援既有 Google Drive 的「我的雲端硬碟」資料夾。若要改檔案位置，可指定 `--oauth-client`／`--oauth-token`，之後執行時也須使用相同路徑。

### 2. 確認共享根目錄權限並準備連結

本次考試目錄與備份目錄都已備妥。

1. 開啟本課程的 Google Drive 共享根目錄，點選「共用」，確認 `credentials.json` 的 `client_email` 對應的 Google 服務帳戶為 **編輯者**。
2. 開啟既有的本次考試目錄，在其中建立 Google 試算表。
3. 將試算表的一般存取權設為 **知道連結的任何人 → 檢視者**，複製試算表網址。
4. 開啟既有的本次考試備份目錄，複製網址。

本次考試目錄及其中檔案繼承共享根目錄的編輯權限。[Google 資料夾權限繼承說明](https://support.google.com/drive/answer/7166529?hl=en)

### 3. 放入 Microsoft Excel 名冊與考試設定檔，執行初始化指令

將檔案放在專案資料夾，以下使用這些檔名：

`roster.xlsx` 與 `exam-config.json` 都由助教提供，並確認是本次考試使用的版本。

```text
ntut-exam-score-gsheet-syncer/
  main.py
  credentials.json
  drive_oauth_client.json
  drive_token.json
  roster.xlsx
  exam-config.json
  .env.template
```

- `roster.xlsx`：Microsoft Excel 名冊。選定的工作表第一列須包含 `班級`、`姓名`、`學號`，欄位順序不限；其他課程成績不會匯入。學號保存為文字，學生依名冊順序排列，缺漏或重複學號會中止初始化。
- `exam-config.json`：這次考試的 **完整考試設定檔**，須包含 `sections[].puzzles[]`、子題配分等資料；不是只有連線／時間選項的學生端設定。
- 下列以名冊工作表 `工作表1` 為例；請換成實際名稱。如果多張工作表都有身分欄位，必須明確指定 `--roster-sheet`。

先預覽。將 `TARGET_ID` 換成第 2 步建立的試算表識別碼，或直接貼完整網址。

PowerShell：

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --config "exam-config.json" `
  --roster "roster.xlsx" `
  --roster-sheet "工作表1" `
  --dry-run
```

Bash：

```bash
uv run python main.py --setup-sheet \
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" \
  --config "exam-config.json" \
  --roster "roster.xlsx" \
  --roster-sheet "工作表1" \
  --dry-run
```

確認目的地與名冊無誤後，執行真正的初始化。

PowerShell：

```powershell
uv run python main.py --setup-sheet `
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" `
  --config "exam-config.json" `
  --roster "roster.xlsx" `
  --roster-sheet "工作表1"
```

Bash：

```bash
uv run python main.py --setup-sheet \
  --sheet-url "https://docs.google.com/spreadsheets/d/TARGET_ID/edit" \
  --config "exam-config.json" \
  --roster "roster.xlsx" \
  --roster-sheet "工作表1"
```

程式會建立或初始化 `1222`、`1223`、`335`、`Combined`、`成績登記` 五個分頁，包含名冊、跨頁公式、配分／規則、分區與最終成績、統計與折線圖。它只修改指定試算表，不會替你建立新的 Google 檔案。

初始化一定要明確提供 `--sheet-url`、`--config`、`--roster`；此時不必先建立 `.env`。若目標分頁已有資料、公式或圖表，會先顯示摘要並警告，只有輸入 `y`／`yes` 才覆寫；直接 Enter、拒絕或輸入結束都會取消。`--dry-run` 只讀取與預覽。

同一份共用試算表只需初始化一次。**重新初始化會清掉目標分頁的既有成績；考試開始後不要把建表當成日常同步指令。** 需要重建時，先停止各教室的同步程序。其他無關分頁不會被重建。

### 4. 將 Google Drive／Google 試算表連結填入 .env，驗證並啟動

#### 考前：填寫 .env

本階段準備設定檔，可先取得並填入後端網址與管理權杖。

首次部署先從範本建立 `.env`。以下指令會保留已存在的檔案；若 `.env` 已存在，直接編輯即可。

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

以 `1222` 教室為例填寫下列設定；連結與後端資訊都換成實際值：

```dotenv
# 分數試算表網址與本教室的來源分頁
SHEET_URL=https://docs.google.com/spreadsheets/d/TARGET_ID/edit
SHEET_TITLE=1222
# 分數同步間隔（秒），持續執行時填正數
INTERVAL=60

# 本教室 `ntut-exam-v2` 後端網址與管理權杖；資料庫備份也使用管理應用程式介面
BACKEND_API_URL=http://localhost:3000
ADMIN_TOKEN=replace-with-the-backend-admin-token

# Google Drive 的「我的雲端硬碟」 備份資料夾網址、間隔（秒）與備份檔名中的教室名稱
BACKUP_FOLDER=https://drive.google.com/drive/u/5/folders/FOLDER_ID
BACKUP_INTERVAL=300
CLASSROOM=1222

# 後端下次退避等待的停止門檻（秒），不是總執行時間
BACKOFF_MAX_SECONDS=10
# Google 每次連線／讀取等待的逾時時間（秒）
GOOGLE_TIMEOUT_SECONDS=30
```

| 變數 | 簡單說明 |
| --- | --- |
| `SHEET_URL` | 第 2 步建立的試算表網址。 |
| `SHEET_TITLE` | 本教室的來源分頁：`1222`、`1223` 或 `335`；不可指向 `Combined`／`成績登記`。 |
| `INTERVAL` | 分數同步間隔，填正數持續輪詢；以下使用 60 秒。 |
| `BACKEND_API_URL` | 對應本教室的後端根網址，不加 `/admin/scores` 或 `/admin/backup`。 |
| `ADMIN_TOKEN` | 後端管理權杖，供程式讀取分數與下載資料庫。 |
| `BACKUP_FOLDER` | 備份資料夾網址或識別碼；留空會停用備份。支援含 `/drive/u/5/folders/` 的網址。 |
| `BACKUP_INTERVAL` | 備份間隔，填正數持續備份，預設 300 秒。與分數同步計時分開。 |
| `CLASSROOM` | 備份檔名的教室名稱；未填時使用 `SHEET_TITLE`。 |
| `BACKOFF_MAX_SECONDS` | 後端退避上限，預設 10 秒：等待 1、2、4、8 秒後，下一次若需等 16 秒就報錯停止。 |
| `GOOGLE_TIMEOUT_SECONDS` | Google 試算表／Google Drive／Google 帳號授權權杖每次網路請求的逾時時間，預設 30 秒；不限制瀏覽器登入時間。 |

修改 `.env` 後重新啟動程序。`BACKOFF_MAX_SECONDS`、`GOOGLE_TIMEOUT_SECONDS` 須為大於 0 的有限數字；未設定或空白採用預設值。

#### 考試時部署：連線後端、預覽並持續執行

如果後端 `/admin/scores` 還沒有任何成績資料列，目前同步器會回報缺少必要欄位並退出，分數預覽也一樣。請先確認後端已完成必要初始化並有成績資料，再執行預覽與啟動；資料庫備份可先單獨部署並驗證。

確認後端正在執行，接著先執行預覽，檢查預計更新內容與備份目的地；預覽完成後，再啟動持續同步與備份。以下 `.env` 已設定 `BACKUP_FOLDER`，預覽模式會完成一輪後結束。

PowerShell：

```powershell
uv run python main.py --config "exam-config.json" --dry-run
uv run python main.py --config "exam-config.json"
```

Bash：

```bash
uv run python main.py --config "exam-config.json" --dry-run
uv run python main.py --config "exam-config.json"
```

第一行會讀取後端與 Google 試算表，列出預計分數變更與備份計畫。第二行會立即同步／備份，之後依 `.env` 的兩個正數間隔持續執行。啟動後核對分數及 `Uploaded` 檔案網址，再確認後續週期；實際備份成功用來驗證後端下載及 Google Drive 上傳權限。需要停止時按 Ctrl+C；程序必須保持執行，本程式不會自動安裝背景服務。

每個程序只負責一個教室／後端。同一教室只保留一個分數寫入程序。若要同時部署三個教室，分別準備獨立的部署資料夾與 `.env`，共用同一份 `SHEET_URL`，但將 `SHEET_TITLE`、`CLASSROOM`、後端網址及權杖設為各教室對應值；不要重複初始化共用試算表。

## 同步與計分行為

- 建表與同步使用同一份 `--config`。題號依 `sections[].puzzles[]` 的設定順序排列，子題也保持設定順序；同時支援實際題目識別碼與學生端 `Q1...Qn`。助教的預覽與同步流程統一指定本次 `exam-config.json`。
- 可用 `--question-map mapping.json` 覆蓋題目對應，但仍須提供 `--config`。設定檔案以實際識別碼對應從 1 起算的工作表題號，例如 `{"custom-question-b": 2, "custom-question-a": 1}`。未知識別碼、重複目的地、歧義或考試設定檔／分頁結構不符會在寫入前停止。
- 只上傳有變化的儲存格。一般答錯會清除舊的通過旗標；沒有規則結果的已存在題目採不扣分。後端有、名冊沒有的學生會警告並跳過。
- `SE`、`CTLE`、`ABORTED`、系統歸因及 `ruleId: "__engine_error__"` 會跳過該學生的該題、保留原資料並印出原因；其他題目與學生繼續同步。規則引擎錯誤優先顯示具體 `reason`。
- `Combined` 依學號加總三個教室的結果，不依賴列的位置，也不把重複結果壓成 1。大於 1 的值保留並標色，供檢查重複應試或輸入錯誤。
- `成績登記` 套用考試設定檔的實際子題配分、每題一次的規則倍率與分區上限，最後才做一次四捨五入；不額外限制總分為 100。原始分數不套規則扣分與分區上限。
- 外觀沿用[參考試算表](https://docs.google.com/spreadsheets/d/12cAocV3Pp6FU-5aDu_zFSh2Gyh3b2x8V2qWdwx0huCY/edit)：白底、黑色分組框線、緊湊欄寬、微軟正黑體分數、紅綠最終成績、隱藏零值文字及灰色原始分數。右側包含及格／不及格統計、十二個分數區間、累計人數，以及 338 × 209 的雙數列折線圖。題目數與配分依考試設定檔調整。

## 備份與錯誤處理

後端的 `GET /admin/backup` 管理應用程式介面回傳二進位 `.sqlite` 快照，格式為 SQLite 資料庫檔案。檔名為 `classroom_timestamp.sqlite`，例如 `1222_20261026T143000123456+0800.sqlite`，使用臺北時區與微秒時間。首次立即備份，後續在上次完成後等待 `BACKUP_INTERVAL`；同步與備份共用程序時依序執行，不會重疊。

下載後檢查完整性、SQLite 檔頭與唯讀完整性檢查，再原樣上傳。Google 官方軟體開發套件更新認證後重送會保留相同 Google Drive 檔案識別碼與完整內容。若上傳回應遺失或截斷，會依該識別碼核對檔名、目的地、大小與檔案雜湊值，不盲目重傳。驗證失敗就報錯；暫存檔會清理，既有雲端備份保留。[Google 上傳說明](https://developers.google.com/workspace/drive/api/guides/manage-uploads)

後端錯誤依 `BACKOFF_MAX_SECONDS` 決定是否繼續退避重試。這不是所有嘗試加總的時間；每次後端請求另外使用 10 秒的連線／讀取逾時時間，持續有資料傳輸的下載可以超過 10 秒。Google 認證重試交給 Google 官方軟體開發套件，不套用後端退避重試；`GOOGLE_TIMEOUT_SECONDS` 也不是整段 Google 操作的總時間。

錯誤會顯示網路請求／服務端／傳輸層的可用原因並遮蔽權杖；重試用盡、Google 請求失敗或備份無法驗證時，包含常駐模式在內都會以非零狀態停止。Google 帳號授權過期／撤銷時重新執行第 1 步登入；權限錯誤時確認檔案位於共享根目錄內，再由維護考試系統的同學處理。

若只想單獨執行週期備份，先用 `--backup-only --dry-run` 預覽，再使用下列指令持續執行；資料夾與正數間隔採 `.env` 設定。只需分數同步時，將 `.env` 的 `BACKUP_FOLDER` 留空、`INTERVAL=60`，同步指令保留 `--config "exam-config.json"`。此模式的預覽也會持續輪詢，檢查後按 Ctrl+C，再啟動正式同步。

PowerShell：

```powershell
uv run python main.py --backup-only
```

Bash：

```bash
uv run python main.py --backup-only
```

## 開發驗證

PowerShell：

```powershell
uv run python -m coverage run --branch -m pytest tests -q
uv run python -m coverage json -o coverage.json
uv run python scripts/check_coverage.py coverage.json
git diff --check
```

Bash：

```bash
uv run python -m coverage run --branch -m pytest tests -q
uv run python -m coverage json -o coverage.json
uv run python scripts/check_coverage.py coverage.json
git diff --check
```

離線測試隔離憑證與網路／Google 試算表邊界。測試覆蓋率檢查要求所有正式程式模組的陳述與分支皆為 100%，也會檢查是否有漏測模組；舊版手動診斷 `test_db_parse.py` 不納入離線套件。原生 Google 試算表公式驗證使用可拋棄試算表與合成資料。

設計與驗證紀錄：[架構決策紀錄](docs/decisions/001-sheet-setup-from-roster-and-exam-config.md)、[計畫](tasks/plan.md)、[任務紀錄](tasks/todo.md)。

憑證相關維護流程：[首次建立 Google 服務帳戶](docs/ta-operation-guide.md#appendix-a)、[首次建立 Google Drive 登入授權應用程式憑證](docs/ta-operation-guide.md#appendix-b)、[維護與更新憑證](docs/ta-operation-guide.md#appendix-c)。
