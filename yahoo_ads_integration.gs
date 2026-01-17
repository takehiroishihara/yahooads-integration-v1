/**
 * Yahoo!広告 API - 統合スクリプト (BigQuery出力版) 完全修正版
 *
 * MCC配下の全アカウントから各種データを取得し、BigQueryのテーブルに出力します。
 *
 * 【修正内容】
 * - 各アカウント処理前に認証トークンを再取得
 * - レポート作成後に待機時間を追加
 * - リトライロジックを改善
 * - 全レポートタイプに修正を適用
 */

// ===========================================
// 共通設定
// ===========================================

const CONFIG = {
  // MCCアカウントID（ベースアカウント）
  BASE_ACCOUNT_ID: '1002727355',

  // アプリケーション認証情報
  CLIENT_ID: 'bed60b7aa2f3e6ec78d085e07b4a0dbc55640aca7ecf3054952afd1f5a2f63ff',
  CLIENT_SECRET: '11d1b98254df130b11bcc4bf921f0e41b90a61926b3c55f0bb1eaa6cc08da0b7',
  REFRESH_TOKEN: 'e288896f8ba73fde7d56d85322d62012e36825a2c7f947a88ecfb9c643e94d92',

  // API設定
  DISPLAY_API_BASE: 'https://ads-display.yahooapis.jp/api/v16',
  SEARCH_API_BASE: 'https://ads-search.yahooapis.jp/api/v14',
  AUTH_URL: 'https://biz-oauth.yahoo.co.jp/oauth/v1/token',

  // レポート設定
  DAY_COUNT: 45,
  INCLUDE_TODAY: false,

  // BigQuery設定
  BQ_PROJECT_ID: 'model-zoo-484006-m6',
  BQ_DATASET_ID: 'yahoo_ads_raw',

  // BigQueryテーブル名設定
  TABLES: {
    ACCOUNT_LIST: 'account_list',
    CAMPAIGN: 'campaign_settings',
    ADGROUP: 'adgroup_settings',
    AD: 'ad_report',
    MEDIA: 'media_master',
    PLACEMENT: 'placement_report',
    GENDER: 'gender_report',
    AGE: 'age_report',
    DEVICE: 'device_report'
  }
};

// ===========================================
// BigQuery 転送用共通関数
// ===========================================

/**
 * 2次元配列データをCSVに変換してBigQueryにロードする
 */
function loadToBigQuery_(tableId, dataHeader, dataBody) {
  if (!dataBody || dataBody.length === 0) {
    log_(`⚠ ${tableId}: データがないためスキップします`);
    return;
  }

  log_(`🚀 BigQuery転送開始: ${tableId} (${dataBody.length}件)`);

  const allData = [dataHeader, ...dataBody];

  const csvString = allData.map(row => {
    return row.map(cell => {
      const str = String(cell);
      if (str.includes('"') || str.includes(',') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    }).join(',');
  }).join('\n');

  const blob = Utilities.newBlob(csvString, 'application/octet-stream');

  const job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: CONFIG.BQ_PROJECT_ID,
          datasetId: CONFIG.BQ_DATASET_ID,
          tableId: tableId
        },
        writeDisposition: 'WRITE_TRUNCATE',
        createDisposition: 'CREATE_IF_NEEDED',
        sourceFormat: 'CSV',
        autodetect: true,
        skipLeadingRows: 1
      }
    }
  };

  try {
    const insertJob = BigQuery.Jobs.insert(job, CONFIG.BQ_PROJECT_ID, blob);
    log_(`✅ BigQueryジョブ投入成功: JobId ${insertJob.jobReference.jobId}`);
  } catch (e) {
    log_(`❌ BigQuery転送エラー: ${e.message}`);
    throw e;
  }
}

// ===========================================
// 共通ユーティリティ関数
// ===========================================

/**
 * ログ出力
 */
function log_(message) {
  Logger.log(message);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName('ログ');
    if (logSheet) {
      const now = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy-MM-dd HH:mm:ss');
      logSheet.appendRow([now, message]);
    }
  } catch (e) {
    // ログ出力エラーは無視
  }
}

/**
 * アクセストークン取得
 */
function getAccessToken_() {
  const payload = {
    grant_type: 'refresh_token',
    client_id: CONFIG.CLIENT_ID,
    client_secret: CONFIG.CLIENT_SECRET,
    refresh_token: CONFIG.REFRESH_TOKEN
  };

  const options = {
    method: 'POST',
    payload: payload,
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(CONFIG.AUTH_URL, options);
    const status = response.getResponseCode();
    const content = response.getContentText();

    if (status === 200) {
      const json = JSON.parse(content);
      return json.access_token;
    } else {
      log_(`❌ 認証エラー(${status}): ${content}`);
      return null;
    }
  } catch (e) {
    log_(`❌ 認証例外: ${e.message}`);
    return null;
  }
}

/**
 * 認証ヘッダー取得
 */
function getAuthHeaders_() {
  const accessToken = getAccessToken_();
  if (!accessToken) return null;

  return {
    'Authorization': `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'x-z-base-account-id': String(CONFIG.BASE_ACCOUNT_ID)
  };
}

/**
 * 日付範囲計算
 */
function getDateRange_(dayCount, includeToday) {
  const now = new Date();
  const end = new Date(now);

  if (!includeToday) {
    end.setDate(end.getDate() - 1);
  }

  const start = new Date(end);
  start.setDate(start.getDate() - (dayCount - 1));

  const startStr = Utilities.formatDate(start, 'Asia/Tokyo', 'yyyyMMdd');
  const endStr = Utilities.formatDate(end, 'Asia/Tokyo', 'yyyyMMdd');

  return { startStr, endStr };
}

/**
 * CSV解析
 */
function parseCsv_(csvText) {
  if (!csvText) return [];

  const lines = csvText
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .split('\n')
    .filter(line => line.trim() !== '');

  return lines.map(line => {
    const cleaned = line.replace(/"/g, '');
    return cleaned.split(',');
  });
}

/**
 * 対象アカウント一覧を取得（シートまたはAPI）
 */
function getTargetAccounts_(apiBase) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('MCC配下アカウント一覧');

  if (sheet && sheet.getLastRow() >= 2) {
    const lastRow = sheet.getLastRow();
    const data = sheet.getRange(2, 1, lastRow - 1, 3).getValues();
    const accounts = [];

    const targetType = apiBase === CONFIG.SEARCH_API_BASE ? '検索広告' : 'ディスプレイ広告';

    data.forEach(row => {
      const type = row[0];
      const id = row[1];
      const name = row[2];

      if (type === targetType && id) {
        accounts.push({
          accountId: String(id).trim(),
          accountName: name || ''
        });
      }
    });

    if (accounts.length > 0) return accounts;
  }

  log_('📡 アカウント一覧をAPIから取得');
  return getAccountsFromApi_(apiBase);
}

/**
 * APIからアカウント一覧を取得
 */
function getAccountsFromApi_(apiBase) {
  const headers = getAuthHeaders_();
  if (!headers) return [];

  const url = `${apiBase}/AccountLinkService/get`;
  const payload = { mccAccountId: parseInt(CONFIG.BASE_ACCOUNT_ID) };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const json = JSON.parse(response.getContentText());

    const accounts = [];
    (json.rval?.values || []).forEach(v => {
      if (v.accountLink) {
        accounts.push({
          accountId: String(v.accountLink.accountId),
          accountName: v.accountLink.accountName || '',
          ownerShipType: v.accountLink.ownerShipType || ''
        });
      }
    });

    return accounts;
  } catch (e) {
    log_(`❌ アカウント一覧取得エラー: ${e.message}`);
    return [];
  }
}

// ===========================================
// レポート定義作成・ダウンロード（修正版）
// ===========================================

/**
 * レポート定義作成
 */
function createReportDefinition_(accountId, startDate, endDate, headers, fields, reportName) {
  const url = `${CONFIG.DISPLAY_API_BASE}/ReportDefinitionService/add`;

  const body = {
    accountId: Number(accountId),
    operand: [{
      reportName: `${reportName}_${startDate}_${endDate}_${Date.now()}`,
      reportDateRangeType: "CUSTOM_DATE",
      dateRange: {
        startDate: startDate,
        endDate: endDate
      },
      fields: fields,
      sortFields: [{
        field: "DAY",
        reportSortType: "ASC"
      }],
      reportDownloadFormat: "CSV",
      reportDownloadEncode: "UTF8",
      reportCompressType: "NONE",
      reportLanguage: "JA"
    }]
  };

  const options = {
    method: 'POST',
    headers: headers,
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const status = response.getResponseCode();
  const text = response.getContentText();

  if (status !== 200) {
    throw new Error(`ReportDefinition作成エラー(${status}): ${text.substring(0, 300)}`);
  }

  const json = JSON.parse(text);
  const val = json?.rval?.values?.[0];

  if (!val?.operationSucceeded) {
    throw new Error(`ReportDefinition作成失敗: ${JSON.stringify(json.errors || json).substring(0, 300)}`);
  }

  return val.reportDefinition.reportJobId;
}

/**
 * レポートダウンロード（改善版）
 */
function downloadReportWithRetry_(accountId, jobId, headers) {
  const url = `${CONFIG.DISPLAY_API_BASE}/ReportDefinitionService/download`;

  const payload = JSON.stringify({
    accountId: Number(accountId),
    reportJobId: jobId
  });

  const maxRetry = 30;
  const waitMs = 3000;

  for (let i = 0; i < maxRetry; i++) {
    try {
      const response = UrlFetchApp.fetch(url, {
        method: 'POST',
        headers: headers,
        payload: payload,
        muteHttpExceptions: true
      });

      const code = response.getResponseCode();
      const content = response.getContentText();

      // 成功かつデータがある場合
      if (code === 200 && content.length > 100) {
        return content;
      }

      // 200だけどデータが少ない（まだ生成中の可能性）
      if (code === 200 && content.length <= 100) {
        Utilities.sleep(waitMs);
        continue;
      }

      // レポート生成中
      if (code === 404 || code === 409) {
        Utilities.sleep(waitMs);
        continue;
      }

      // 認証エラー
      if (code === 401) {
        throw new Error(`認証エラー(401): ${content.substring(0, 200)}`);
      }

      // その他のエラー
      Utilities.sleep(waitMs);

    } catch (e) {
      if (e.message.includes('認証エラー')) throw e;
      if (i === maxRetry - 1) throw e;
      Utilities.sleep(waitMs);
    }
  }

  // タイムアウトの場合は空を返す
  return '';
}

// ===========================================
// 1. アカウント一覧取得
// ===========================================

function getMccChildAccounts() {
  log_('🚀 MCC配下アカウント取得開始');

  const headers = getAuthHeaders_();
  if (!headers) {
    log_('❌ 認証失敗');
    return;
  }

  log_('\n=== ディスプレイ広告 ===');
  const displayAccounts = getAccountsWithNames_(CONFIG.DISPLAY_API_BASE, headers, 'ディスプレイ広告');

  Utilities.sleep(500);

  log_('\n=== 検索広告 ===');
  const searchAccounts = getAccountsWithNames_(CONFIG.SEARCH_API_BASE, headers, '検索広告');

  const allAccounts = displayAccounts.concat(searchAccounts);
  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  log_(`\n✅ ディスプレイ: ${displayAccounts.length}件, 検索: ${searchAccounts.length}件`);

  const bqHeader = ['種別', 'アカウントID', 'アカウント名', '所有権タイプ', 'ステータス', '取得日時'];
  const bqData = allAccounts.map(acc => [
    acc.accountType,
    acc.accountId,
    acc.accountName,
    acc.ownerShipType,
    acc.accountStatus,
    timestamp
  ]);

  loadToBigQuery_(CONFIG.TABLES.ACCOUNT_LIST, bqHeader, bqData);
  outputAccountsToSheet_(displayAccounts, searchAccounts);

  return { display: displayAccounts, search: searchAccounts };
}

function getAccountsWithNames_(apiBase, headers, label) {
  const accountIds = getAccountLinks_(apiBase, headers);
  if (accountIds.length === 0) return [];

  Utilities.sleep(300);

  const accountDetails = getAccountDetails_(apiBase, headers, accountIds);

  return accountIds.map(acc => {
    const detail = accountDetails.find(d => String(d.accountId) === String(acc.accountId));
    return {
      accountId: acc.accountId,
      accountName: detail?.accountName || acc.accountName || '',
      ownerShipType: acc.ownerShipType,
      accountStatus: detail?.accountStatus || '',
      accountType: label
    };
  });
}

function getAccountLinks_(apiBase, headers) {
  const url = `${apiBase}/AccountLinkService/get`;
  const payload = { mccAccountId: parseInt(CONFIG.BASE_ACCOUNT_ID) };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const json = JSON.parse(response.getContentText());
    const accounts = [];

    (json.rval?.values || []).forEach(v => {
      if (v.accountLink) {
        accounts.push({
          accountId: v.accountLink.accountId,
          accountName: v.accountLink.accountName || '',
          ownerShipType: v.accountLink.ownerShipType || ''
        });
      }
    });

    return accounts;
  } catch (e) {
    log_(`❌ AccountLinkService エラー: ${e.message}`);
    return [];
  }
}

function getAccountDetails_(apiBase, headers, accounts) {
  const url = `${apiBase}/AccountService/get`;
  const accountIds = accounts.map(a => parseInt(a.accountId));

  const payload = { accountIds: accountIds };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const json = JSON.parse(response.getContentText());
    const details = [];

    (json.rval?.values || []).forEach(v => {
      if (v.account) {
        details.push({
          accountId: v.account.accountId,
          accountName: v.account.accountName || '',
          accountStatus: v.account.accountStatus || ''
        });
      }
    });

    return details;
  } catch (e) {
    log_(`❌ AccountService エラー: ${e.message}`);
    return [];
  }
}

function outputAccountsToSheet_(displayAccounts, searchAccounts) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName('MCC配下アカウント一覧');

  if (!sheet) {
    sheet = ss.insertSheet('MCC配下アカウント一覧');
  }

  sheet.clear();

  const headers = ['種別', 'アカウントID', 'アカウント名', '所有権タイプ', 'ステータス', '取得日時'];
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight('bold');

  const allAccounts = displayAccounts.concat(searchAccounts);
  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  if (allAccounts.length > 0) {
    const data = allAccounts.map(acc => [
      acc.accountType,
      acc.accountId,
      acc.accountName,
      acc.ownerShipType,
      acc.accountStatus,
      timestamp
    ]);
    sheet.getRange(2, 1, data.length, headers.length).setValues(data);
  }

  log_(`📊 シート「MCC配下アカウント一覧」に${allAccounts.length}件出力完了`);
}

// ===========================================
// 2. キャンペーン設定取得
// ===========================================

function getCampaignSettings() {
  log_('🚀 キャンペーン設定取得開始');

  const accounts = getTargetAccounts_(CONFIG.DISPLAY_API_BASE);
  if (accounts.length === 0) {
    log_('⚠ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  const allCampaignSettings = [];

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`[${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    // 毎回認証ヘッダーを取得
    const headers = getAuthHeaders_();
    if (!headers) {
      log_(`  ❌ 認証失敗 - スキップ`);
      continue;
    }

    const settings = getCampaignSettingsForAccount_(
      CONFIG.DISPLAY_API_BASE,
      headers,
      account.accountId,
      account.accountName
    );

    if (settings.length > 0) {
      log_(`  ✅ ${settings.length}件取得`);
      allCampaignSettings.push(...settings);
    } else {
      log_(`  ⏭ 0件`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(500);
    }
  }

  log_(`\n✅ キャンペーン総数: ${allCampaignSettings.length}件`);

  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  const bqHeader = [
    'アカウントID', 'アカウント名', 'キャンペーンID', 'キャンペーン名',
    'ユーザーステータス', '配信ステータス', 'キャンペーンゴール', 'キャンペーンタイプ',
    '開始日', '終了日', '予算_日額', '予算配信方法',
    '入札戦略タイプ', 'Max_CPC', 'Max_CPV', 'Target_CPA', 'Target_ROAS',
    'FC_レベル', 'FC_期間単位', 'FC_インプレッション数',
    '最適化フラグ', '作成日', '取得日時'
  ];

  const bqData = allCampaignSettings.map(c => [
    c.accountId, c.accountName, c.campaignId, c.campaignName,
    c.userStatus, c.servingStatus, c.campaignGoal, c.campaignType,
    c.startDate, c.endDate, c.budgetAmount, c.deliveryMethod,
    c.biddingStrategyType, c.maxCpcBidValue, c.maxCpvBidValue,
    c.targetCpaBidValue, c.targetRoasBidValue,
    c.frequencyLevel, c.frequencyTimeUnit, c.frequencyImpressions,
    c.conversionOptimizerFlag, c.createdDate, timestamp
  ]);

  loadToBigQuery_(CONFIG.TABLES.CAMPAIGN, bqHeader, bqData);

  return allCampaignSettings;
}

function getCampaignSettingsForAccount_(apiBase, headers, accountId, accountName) {
  const url = `${apiBase}/CampaignService/get`;
  const payload = { accountId: parseInt(accountId) };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const status = response.getResponseCode();

    if (status !== 200) {
      return [];
    }

    const json = JSON.parse(response.getContentText());
    const campaigns = [];

    (json.rval?.values || []).forEach(v => {
      const c = v.campaign;
      if (c) {
        const bid = c.campaignBiddingStrategy || {};
        const freq = c.viewableFrequencyCap || c.frequencyCap || {};
        const bud = c.budget || {};
        const opt = c.conversionOptimizer || {};

        campaigns.push({
          accountId: accountId,
          accountName: accountName,
          campaignId: c.campaignId || '',
          campaignName: c.campaignName || '',
          userStatus: c.userStatus || '',
          servingStatus: c.servingStatus || '',
          campaignGoal: c.campaignGoal || '',
          campaignType: c.campaignType || c.type || '',
          startDate: c.startDate || '',
          endDate: c.endDate || '',
          budgetAmount: bud.amount || '',
          deliveryMethod: bud.deliveryMethod || '',
          biddingStrategyType: bid.campaignBiddingStrategyType || '',
          maxCpcBidValue: bid.maxCpcBidValue || '',
          maxCpvBidValue: bid.maxCpvBidValue || '',
          targetCpaBidValue: bid.targetCpaBidValue || '',
          targetRoasBidValue: bid.targetRoasBidValue || '',
          frequencyLevel: freq.frequencyLevel || freq.level || '',
          frequencyTimeUnit: freq.frequencyTimeUnit || freq.timeUnit || '',
          frequencyImpressions: freq.vImps || freq.impression || '',
          conversionOptimizerFlag: opt.conversionOptimizerEligibilityFlg || '',
          createdDate: c.createdDate || ''
        });
      }
    });

    return campaigns;
  } catch (e) {
    log_(`  ❌ CampaignService エラー: ${e.message}`);
    return [];
  }
}

// ===========================================
// 3. 広告グループ取得
// ===========================================

function getAdGroups() {
  log_('🚀 広告グループ一覧取得開始');

  const accounts = getTargetAccounts_(CONFIG.DISPLAY_API_BASE);
  if (accounts.length === 0) {
    log_('⚠ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  const allAdGroups = [];

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`[${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    // 毎回認証ヘッダーを取得
    const headers = getAuthHeaders_();
    if (!headers) {
      log_(`  ❌ 認証失敗 - スキップ`);
      continue;
    }

    const groups = getAdGroupsForAccount_(
      CONFIG.DISPLAY_API_BASE,
      headers,
      account.accountId,
      account.accountName
    );

    if (groups.length > 0) {
      log_(`  ✅ ${groups.length}件取得`);
      allAdGroups.push(...groups);
    } else {
      log_(`  ⏭ 0件`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(500);
    }
  }

  log_(`\n✅ 広告グループ総数: ${allAdGroups.length}件`);

  const timestamp = Utilities.formatDate(new Date(), 'Asia/Tokyo', 'yyyy/MM/dd HH:mm:ss');

  const bqHeader = [
    'アカウントID', 'アカウント名', 'キャンペーンID', 'キャンペーン名',
    '広告グループID', '広告グループ名', 'ユーザーステータス',
    '入札戦略タイプ', 'CPC入札値', 'CPV入札値', 'Target_CPA', 'Target_ROAS',
    'デバイス', 'デバイスアプリ', 'デバイスOS', 'スマートターゲティング',
    'フィードセットID', '作成日', '更新日', '取得日時'
  ];

  const bqData = allAdGroups.map(ag => [
    ag.accountId, ag.accountName, ag.campaignId, ag.campaignName,
    ag.adGroupId, ag.adGroupName, ag.userStatus,
    ag.campaignBiddingStrategyType, ag.cpcBidValue, ag.cpvBidValue,
    ag.targetCpa, ag.targetRoas,
    ag.device, ag.deviceApp, ag.deviceOs, ag.smartTargetingEnabled,
    ag.feedSetId, ag.createdDate, ag.updatedDate, timestamp
  ]);

  loadToBigQuery_(CONFIG.TABLES.ADGROUP, bqHeader, bqData);

  return allAdGroups;
}

function getAdGroupsForAccount_(apiBase, headers, accountId, accountName) {
  const url = `${apiBase}/AdGroupService/get`;
  const payload = { accountId: parseInt(accountId) };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options);
    const status = response.getResponseCode();

    if (status !== 200) {
      return [];
    }

    const json = JSON.parse(response.getContentText());
    const groups = [];

    (json.rval?.values || []).forEach(v => {
      const ag = v.adGroup;
      if (ag) {
        const bid = ag.biddingStrategyConfiguration?.biddingScheme || {};
        const dev = ag.device || [];
        const devApp = ag.deviceApp || [];
        const devOs = ag.deviceOs || [];

        groups.push({
          accountId: accountId,
          accountName: accountName,
          campaignId: ag.campaignId || '',
          campaignName: ag.campaignName || '',
          adGroupId: ag.adGroupId || '',
          adGroupName: ag.adGroupName || '',
          userStatus: ag.userStatus || '',
          campaignBiddingStrategyType: bid.campaignBiddingStrategyType || '',
          cpcBidValue: bid.cpcBiddingScheme?.cpc || '',
          cpvBidValue: bid.cpvBiddingScheme?.cpv || '',
          targetCpa: bid.maximizeConversionsBiddingScheme?.targetCpa || '',
          targetRoas: bid.maximizeConversionValueBiddingScheme?.targetRoas || '',
          device: Array.isArray(dev) ? dev.join(',') : dev,
          deviceApp: Array.isArray(devApp) ? devApp.join(',') : devApp,
          deviceOs: Array.isArray(devOs) ? devOs.join(',') : devOs,
          smartTargetingEnabled: ag.smartTargetingEnabled || '',
          feedSetId: ag.feedSetId || '',
          createdDate: ag.createdDate || '',
          updatedDate: ag.updatedDate || ''
        });
      }
    });

    return groups;
  } catch (e) {
    log_(`  ❌ AdGroupService エラー: ${e.message}`);
    return [];
  }
}

// ===========================================
// 4. AD単位レポート取得（修正版）
// ===========================================

function fetchAllAccountsAdReport() {
  log_('===== 🚀 全アカウント ADレポート取得開始 =====');

  const accounts = getTargetAccounts_(CONFIG.DISPLAY_API_BASE);
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  let allAdData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    // ★ 毎回認証ヘッダーを新規取得
    const headers = getAuthHeaders_();
    if (!headers) {
      log_(`  ❌ 認証失敗 - スキップ`);
      errorCount++;
      continue;
    }

    try {
      const adData = fetchAdReportForAccount_(account, headers);

      if (adData.length > 0) {
        allAdData = allAdData.concat(adData);
        log_(`  ✅ ${adData.length}件取得 → 累計: ${allAdData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    // ★ アカウント間の待機時間
    if (i < accounts.length - 1) {
      Utilities.sleep(2000);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`総データ件数: ${allAdData.length}件`);

  // アカウント別の件数を表示
  const countByAccount = {};
  allAdData.forEach(row => {
    const accId = row[0];
    countByAccount[accId] = (countByAccount[accId] || 0) + 1;
  });

  log_('\n--- アカウント別件数 ---');
  Object.keys(countByAccount).sort().forEach(accId => {
    log_(`  ${accId}: ${countByAccount[accId]}件`);
  });

  // BigQueryに転送
  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'campaign_name', 'adgroup_id', 'adgroup_name',
    'ad_id', 'ad_name', 'ad_title', 'description1', 'description2', 'media_id',
    'impressions', 'clicks', 'conversions', 'conversions_all', 'conversions_view_through', 'cost', 'avg_rank'
  ];

  loadToBigQuery_(CONFIG.TABLES.AD, bqHeader, allAdData);

  return allAdData;
}

function fetchAdReportForAccount_(account, headers) {
  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);

  const fields = [
    "CAMPAIGN_ID", "CAMPAIGN_NAME", "ADGROUP_ID", "ADGROUP_NAME",
    "AD_ID", "AD_NAME", "AD_TITLE", "DESCRIPTION1", "DESCRIPTION2", "MEDIA_ID",
    "DAY", "IMPS", "COST", "CLICKS", "CONVERSIONS", "ALL_CONV", "AVG_DELIVER_RANK"
  ];

  // レポート定義作成
  const jobId = createReportDefinition_(account.accountId, startStr, endStr, headers, fields, 'ad_report');

  // ★ レポート作成後に少し待機
  Utilities.sleep(2000);

  // レポートダウンロード
  const csvText = downloadReportWithRetry_(account.accountId, jobId, headers);

  if (!csvText || csvText.length < 100) {
    return [];
  }

  const rawRows = parseCsv_(csvText);

  if (rawRows.length < 2) return [];

  return formatAdReportData_(rawRows, account.accountId, account.accountName);
}

function formatAdReportData_(rawRows, accountId, accountName) {
  const header = rawRows[0];

  const idx = {
    DAY: header.indexOf('日'),
    CAMPAIGN_ID: header.indexOf('キャンペーンID'),
    CAMPAIGN_NAME: header.indexOf('キャンペーン名'),
    ADGROUP_ID: header.indexOf('広告グループID'),
    ADGROUP_NAME: header.indexOf('広告グループ名'),
    AD_ID: header.indexOf('広告ID'),
    AD_NAME: header.indexOf('広告名'),
    AD_TITLE: header.indexOf('タイトル'),
    DESC1: header.indexOf('説明文1'),
    DESC2: header.indexOf('説明文2'),
    MEDIA_ID: header.indexOf('メディアID'),
    IMPS: header.indexOf('インプレッション数'),
    CLICKS: header.indexOf('クリック数'),
    COST: header.indexOf('コスト'),
    CONVERSIONS: header.indexOf('コンバージョン数'),
    ALL_CONV: header.indexOf('コンバージョン数（全て）'),
    AVG_RANK: header.indexOf('平均掲載順位')
  };

  const results = [];

  // 最終行（合計行）を除外
  for (let i = 1; i < rawRows.length - 1; i++) {
    const r = rawRows[i];
    if (!r || r.length === 0) continue;

    const conv = idx.CONVERSIONS >= 0 ? Number(r[idx.CONVERSIONS] || 0) : 0;
    const allConv = idx.ALL_CONV >= 0 ? Number(r[idx.ALL_CONV] || 0) : 0;
    const mcv = allConv - conv;

    results.push([
      accountId,
      accountName,
      idx.DAY >= 0 ? r[idx.DAY] : '',
      idx.CAMPAIGN_ID >= 0 ? r[idx.CAMPAIGN_ID] : '',
      idx.CAMPAIGN_NAME >= 0 ? r[idx.CAMPAIGN_NAME] : '',
      idx.ADGROUP_ID >= 0 ? r[idx.ADGROUP_ID] : '',
      idx.ADGROUP_NAME >= 0 ? r[idx.ADGROUP_NAME] : '',
      idx.AD_ID >= 0 ? r[idx.AD_ID] : '',
      idx.AD_NAME >= 0 ? r[idx.AD_NAME] : '',
      idx.AD_TITLE >= 0 ? r[idx.AD_TITLE] : '',
      idx.DESC1 >= 0 ? r[idx.DESC1] : '',
      idx.DESC2 >= 0 ? r[idx.DESC2] : '',
      idx.MEDIA_ID >= 0 ? r[idx.MEDIA_ID] : '',
      idx.IMPS >= 0 ? Number(r[idx.IMPS] || 0) : 0,
      idx.CLICKS >= 0 ? Number(r[idx.CLICKS] || 0) : 0,
      conv,
      allConv,
      mcv,
      idx.COST >= 0 ? Number(r[idx.COST] || 0) : 0,
      idx.AVG_RANK >= 0 ? r[idx.AVG_RANK] : ''
    ]);
  }

  return results;
}

// ===========================================
// 5. メディア一覧取得
// ===========================================

function fetchAllAccountsMedia() {
  log_('===== 🚀 全アカウント メディア一覧取得開始 =====');

  const accounts = getTargetAccounts_(CONFIG.DISPLAY_API_BASE);
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  let allMediaData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    // ★ 毎回認証ヘッダーを新規取得
    const headers = getAuthHeaders_();
    if (!headers) {
      log_(`  ❌ 認証失敗 - スキップ`);
      errorCount++;
      continue;
    }

    try {
      const mediaData = fetchMediaForAccount_(account, headers);

      if (mediaData.length > 0) {
        allMediaData = allMediaData.concat(mediaData);
        log_(`  ✅ ${mediaData.length}件取得`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    if (i < accounts.length - 1) {
      Utilities.sleep(500);
    }
  }

  log_(`\n✅ メディア総数: ${allMediaData.length}件`);

  // ★ 修正: file_name カラムを追加
  const bqHeader = [
    'account_id', 'account_name', 'media_id', 'media_name', 'file_name', 'media_type',
    'aspect_ratio', 'width', 'height', 'file_size', 'playback_time',
    'approval_status', 'logo_flg', 'thumbnail_flg', 'created_date', 'review_application_date'
  ];

  loadToBigQuery_(CONFIG.TABLES.MEDIA, bqHeader, allMediaData);

  return allMediaData;
}

function fetchMediaForAccount_(account, headers) {
  const url = `${CONFIG.DISPLAY_API_BASE}/MediaService/get`;
  const payload = { accountId: Number(account.accountId) };

  const options = {
    method: 'POST',
    contentType: 'application/json',
    headers: headers,
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  const status = response.getResponseCode();

  if (status !== 200) {
    throw new Error(`MediaService エラー(${status})`);
  }

  const json = JSON.parse(response.getContentText());
  const results = [];

  (json.rval?.values || []).forEach(v => {
    const m = v.mediaRecord;
    if (m) {
      const img = m.imageMedia || {};
      const vid = m.videoMedia || {};

      let type = '', asp = '', w = '', h = '', size = '', play = '', fileName = '';

      if (img.mediaType) {
        type = img.mediaType;
        asp = img.aspectRatio || '';
        w = img.width || '';
        h = img.height || '';
        size = img.fileSize || '';
        fileName = img.fileName || '';  // ★ 画像のファイル名を取得
      } else if (vid.mediaType) {
        type = vid.mediaType;
        asp = vid.aspectRatio || '';
        w = vid.width || '';
        h = vid.height || '';
        size = vid.fileSize || '';
        play = vid.playbackTime || '';
        fileName = vid.fileName || '';  // ★ 動画のファイル名を取得
      }

      results.push([
        account.accountId,
        account.accountName,
        m.mediaId || '',
        m.mediaName || '',     // メディア名（管理画面で設定した名前）
        fileName,              // ★ ファイル名（元のアップロードファイル名）を追加
        type,
        asp,
        w,
        h,
        size,
        play,
        m.approvalStatus || '',
        m.logoFlg || '',
        m.thumbnailFlg || '',
        m.createdDate || '',
        m.reviewApplicationDate || ''
      ]);
    }
  });

  return results;
}

// ===========================================
// 6-9. 各種ディメンションレポート（修正版）
// ===========================================

function fetchAllAccountsPlacementReport() {
  const bqHeader = [
    'account_id', 'account_name', 'day',
    'placement_list_id', 'placement_list_name',
    'impressions', 'clicks', 'conversions', 'conversions_all', 'conversions_view_through', 'cost', 'avg_rank'
  ];

  fetchDimensionReport_(
    'PLACEMENT',
    ['DAY', 'PLACEMENT_LIST_ID', 'PLACEMENT_LIST_NAME', 'IMPS', 'CLICKS', 'CONVERSIONS', 'ALL_CONV', 'COST', 'AVG_DELIVER_RANK'],
    CONFIG.TABLES.PLACEMENT,
    bqHeader
  );
}

function fetchAllAccountsGenderReport() {
  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id', 'gender',
    'impressions', 'clicks', 'conversions', 'conversions_all', 'conversions_view_through', 'cost', 'avg_rank'
  ];

  fetchDimensionReport_(
    'GENDER',
    ['DAY', 'CAMPAIGN_ID', 'ADGROUP_ID', 'GENDER', 'IMPS', 'CLICKS', 'CONVERSIONS', 'ALL_CONV', 'COST', 'AVG_DELIVER_RANK'],
    CONFIG.TABLES.GENDER,
    bqHeader
  );
}

function fetchAllAccountsAgeReport() {
  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id', 'age',
    'impressions', 'clicks', 'conversions', 'conversions_all', 'conversions_view_through', 'cost', 'avg_rank'
  ];

  fetchDimensionReport_(
    'AGE',
    ['DAY', 'CAMPAIGN_ID', 'ADGROUP_ID', 'AGE', 'IMPS', 'CLICKS', 'CONVERSIONS', 'ALL_CONV', 'COST', 'AVG_DELIVER_RANK'],
    CONFIG.TABLES.AGE,
    bqHeader
  );
}

function fetchAllAccountsDeviceReport() {
  const bqHeader = [
    'account_id', 'account_name', 'day',
    'campaign_id', 'adgroup_id', 'device',
    'impressions', 'clicks', 'conversions', 'conversions_all', 'conversions_view_through', 'cost', 'avg_rank'
  ];

  fetchDimensionReport_(
    'DEVICE',
    ['DAY', 'CAMPAIGN_ID', 'ADGROUP_ID', 'DEVICE', 'IMPS', 'CLICKS', 'CONVERSIONS', 'ALL_CONV', 'COST', 'AVG_DELIVER_RANK'],
    CONFIG.TABLES.DEVICE,
    bqHeader
  );
}

/**
 * ディメンション別レポート取得（共通・修正版）
 */
function fetchDimensionReport_(reportType, fields, tableName, bqHeader) {
  log_(`===== 🚀 全アカウント ${reportType}レポート取得開始 =====`);

  const accounts = getTargetAccounts_(CONFIG.DISPLAY_API_BASE);
  if (accounts.length === 0) {
    log_('❌ 対象アカウントがありません');
    return;
  }

  log_(`📋 対象アカウント数: ${accounts.length}`);

  let allData = [];
  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < accounts.length; i++) {
    const account = accounts[i];
    log_(`\n▶ [${i + 1}/${accounts.length}] ${account.accountId} (${account.accountName})`);

    // ★ 毎回認証ヘッダーを新規取得
    const headers = getAuthHeaders_();
    if (!headers) {
      log_(`  ❌ 認証失敗 - スキップ`);
      errorCount++;
      continue;
    }

    try {
      const data = fetchDimensionReportForAccount_(account, headers, reportType, fields);

      if (data.length > 0) {
        allData = allData.concat(data);
        log_(`  ✅ ${data.length}件取得 → 累計: ${allData.length}件`);
      } else {
        log_(`  ⏭ データなし`);
      }
      successCount++;

    } catch (e) {
      errorCount++;
      log_(`  ❌ エラー: ${e.message}`);
    }

    // ★ アカウント間の待機時間
    if (i < accounts.length - 1) {
      Utilities.sleep(2000);
    }
  }

  log_(`\n===== 集計結果 =====`);
  log_(`成功: ${successCount}件 / エラー: ${errorCount}件`);
  log_(`${reportType}レポート総数: ${allData.length}件`);

  loadToBigQuery_(tableName, bqHeader, allData);

  return allData;
}

function fetchDimensionReportForAccount_(account, headers, reportType, fields) {
  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);

  // レポート定義作成
  const jobId = createReportDefinition_(
    account.accountId,
    startStr,
    endStr,
    headers,
    fields,
    `${reportType.toLowerCase()}_report`
  );

  // ★ レポート作成後に少し待機
  Utilities.sleep(2000);

  // レポートダウンロード
  const csvText = downloadReportWithRetry_(account.accountId, jobId, headers);

  if (!csvText || csvText.length < 100) {
    return [];
  }

  const rawRows = parseCsv_(csvText);

  if (rawRows.length < 2) return [];

  return formatDimensionReportData_(rawRows, account.accountId, account.accountName, reportType);
}

function formatDimensionReportData_(rawRows, accountId, accountName, reportType) {
  const header = rawRows[0];

  const idx = {
    DAY: header.indexOf('日'),
    CAMPAIGN_ID: header.indexOf('キャンペーンID'),
    ADGROUP_ID: header.indexOf('広告グループID'),
    IMPS: header.indexOf('インプレッション数'),
    CLICKS: header.indexOf('クリック数'),
    CONVERSIONS: header.indexOf('コンバージョン数'),
    ALL_CONV: header.indexOf('コンバージョン数（全て）'),
    COST: header.indexOf('コスト'),
    AVG_RANK: header.indexOf('平均掲載順位')
  };

  let dimIdx = -1;
  let dimLabel = -1;

  switch (reportType) {
    case 'PLACEMENT':
      dimIdx = header.indexOf('プレイスメントリストID');
      dimLabel = header.indexOf('プレイスメントリスト名');
      break;
    case 'GENDER':
      dimIdx = header.indexOf('性別');
      break;
    case 'AGE':
      dimIdx = header.indexOf('年齢');
      break;
    case 'DEVICE':
      dimIdx = header.indexOf('デバイス');
      break;
  }

  const results = [];
  const endIdx = reportType === 'PLACEMENT' ? rawRows.length : rawRows.length - 1;

  for (let i = 1; i < endIdx; i++) {
    const r = rawRows[i];
    if (!r || r.length === 0) continue;

    const conv = idx.CONVERSIONS >= 0 ? Number(r[idx.CONVERSIONS] || 0) : 0;
    const allConv = idx.ALL_CONV >= 0 ? Number(r[idx.ALL_CONV] || 0) : 0;
    const mcv = allConv - conv;

    const row = [
      accountId,
      accountName,
      idx.DAY >= 0 ? r[idx.DAY] : ''
    ];

    if (reportType === 'PLACEMENT') {
      row.push(dimIdx >= 0 ? r[dimIdx] : '');
      row.push(dimLabel >= 0 ? r[dimLabel] : '');
    } else {
      row.push(idx.CAMPAIGN_ID >= 0 ? r[idx.CAMPAIGN_ID] : '');
      row.push(idx.ADGROUP_ID >= 0 ? r[idx.ADGROUP_ID] : '');
      row.push(dimIdx >= 0 ? r[dimIdx] : '');
    }

    row.push(
      idx.IMPS >= 0 ? Number(r[idx.IMPS] || 0) : 0,
      idx.CLICKS >= 0 ? Number(r[idx.CLICKS] || 0) : 0,
      conv,
      allConv,
      mcv,
      idx.COST >= 0 ? Number(r[idx.COST] || 0) : 0,
      idx.AVG_RANK >= 0 ? r[idx.AVG_RANK] : ''
    );

    results.push(row);
  }

  return results;
}

// ===========================================
// 一括実行関数
// ===========================================

/**
 * 全データを一括取得してBigQueryに転送
 */
function fetchAllData() {
  log_('🚀🚀🚀 全データ一括取得（BQ転送）開始 🚀🚀🚀');

  const startTime = new Date();

  try {
    // 1. アカウント一覧
    getMccChildAccounts();
    Utilities.sleep(2000);

    // 2. キャンペーン設定
    getCampaignSettings();
    Utilities.sleep(2000);

    // 3. 広告グループ
    getAdGroups();
    Utilities.sleep(2000);

    // 4. ADレポート
    fetchAllAccountsAdReport();
    Utilities.sleep(2000);

    // 5. メディア一覧
    fetchAllAccountsMedia();
    Utilities.sleep(2000);

    // 6. プレイスメントレポート
    fetchAllAccountsPlacementReport();
    Utilities.sleep(2000);

    // 7. 性別レポート
    fetchAllAccountsGenderReport();
    Utilities.sleep(2000);

    // 8. 年齢レポート
    fetchAllAccountsAgeReport();
    Utilities.sleep(2000);

    // 9. デバイスレポート
    fetchAllAccountsDeviceReport();

  } catch (e) {
    log_(`❌ 致命的エラー: ${e.message}`);
    log_(e.stack);
  }

  const endTime = new Date();
  const duration = Math.round((endTime - startTime) / 1000 / 60);

  log_(`\n🎉🎉🎉 全データ一括取得（BQ転送）完了 🎉🎉🎉`);
  log_(`処理時間: 約${duration}分`);
}

// ===========================================
// 個別テスト用関数
// ===========================================

/**
 * 特定アカウントのADレポートをテスト取得
 */
function testSingleAccountAdReport() {
  const testAccountId = '1002767287';  // テスト対象のアカウントID

  log_(`===== テスト: ${testAccountId} =====`);

  const headers = getAuthHeaders_();
  if (!headers) {
    log_('❌ 認証失敗');
    return;
  }

  const account = {
    accountId: testAccountId,
    accountName: 'テストアカウント'
  };

  try {
    const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
    log_(`📆 対象期間: ${startStr} ～ ${endStr}`);

    const adData = fetchAdReportForAccount_(account, headers);
    log_(`✅ 取得件数: ${adData.length}`);

    if (adData.length > 0) {
      log_('--- 最初の5件 ---');
      adData.slice(0, 5).forEach((row, i) => {
        log_(`[${i + 1}] 日付:${row[2]} 広告ID:${row[7]} 広告名:${row[8]}`);
      });
    }

  } catch (e) {
    log_(`❌ エラー: ${e.message}`);
    log_(e.stack);
  }
}

/**
 * レポートダウンロード詳細デバッグ
 */
function debugReportDownload() {
  const testAccountId = '1002767287';

  log_('===== レポートダウンロード詳細デバッグ =====');

  const headers = getAuthHeaders_();
  if (!headers) {
    log_('❌ 認証失敗');
    return;
  }

  const { startStr, endStr } = getDateRange_(CONFIG.DAY_COUNT, CONFIG.INCLUDE_TODAY);
  log_(`📆 期間: ${startStr} ～ ${endStr}`);

  const fields = [
    "CAMPAIGN_ID", "CAMPAIGN_NAME", "ADGROUP_ID", "ADGROUP_NAME",
    "AD_ID", "AD_NAME", "AD_TITLE", "DESCRIPTION1", "DESCRIPTION2", "MEDIA_ID",
    "DAY", "IMPS", "COST", "CLICKS", "CONVERSIONS", "ALL_CONV", "AVG_DELIVER_RANK"
  ];

  log_('\n📌 Step1: レポート定義作成');
  const jobId = createReportDefinition_(testAccountId, startStr, endStr, headers, fields, 'debug_ad_report');
  log_(`jobId: ${jobId}`);

  log_('\n📌 Step2: レポートダウンロード');

  const url = `${CONFIG.DISPLAY_API_BASE}/ReportDefinitionService/download`;
  const payload = JSON.stringify({
    accountId: Number(testAccountId),
    reportJobId: jobId
  });

  for (let i = 0; i < 25; i++) {
    log_(`  試行 ${i + 1}/25...`);

    const response = UrlFetchApp.fetch(url, {
      method: 'POST',
      headers: headers,
      payload: payload,
      muteHttpExceptions: true
    });

    const code = response.getResponseCode();
    const contentLength = response.getContentText().length;

    log_(`    HTTPステータス: ${code}, コンテンツ長: ${contentLength}`);

    if (code === 200) {
      if (contentLength > 100) {
        log_(`  ✅ 成功！ データサイズ: ${contentLength}`);

        const csvText = response.getContentText();
        const lines = csvText.split('\n').filter(l => l.trim());
        log_(`  CSV行数: ${lines.length}`);
        log_(`  先頭行: ${lines[0].substring(0, 100)}...`);

        return;
      } else {
        log_(`    ⚠ 200だがデータが少ない: ${response.getContentText()}`);
      }
    }

    if (code === 404 || code === 409) {
      log_(`    ⏳ レポート生成中...`);
    } else {
      log_(`    ⚠ その他のステータス: ${response.getContentText().substring(0, 200)}`);
    }

    Utilities.sleep(5000);
  }

  log_('❌ タイムアウト');
}
