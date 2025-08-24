#!/bin/bash
# セキュアな認証設定スクリプト
# 使用方法: ./scripts/setup_secure_auth.sh YOUR_PROJECT_ID YOUR_GITHUB_REPO

set -e

# 色付きログ出力
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# パラメータチェック
if [ $# -ne 2 ]; then
    log_error "使用方法: $0 <GCP_PROJECT_ID> <GITHUB_REPO>"
    log_error "例: $0 my-project-123 username/repository-name"
    exit 1
fi

PROJECT_ID=$1
GITHUB_REPO=$2
SA_NAME="github-actions-data-pipeline"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
POOL_ID="github-actions"
PROVIDER_ID="github"

log_info "セキュアな認証設定を開始します"
log_info "プロジェクト: $PROJECT_ID"
log_info "GitHubリポジトリ: $GITHUB_REPO"

# 1. 現在のプロジェクト設定確認
log_info "GCPプロジェクト設定確認中..."
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null || echo "none")
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    log_warning "プロジェクトを$PROJECT_IDに設定します"
    gcloud config set project $PROJECT_ID
fi

# プロジェクト番号取得
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
log_info "プロジェクト番号: $PROJECT_NUMBER"

# 2. 必要なAPI有効化
log_info "必要なAPIを有効化中..."
gcloud services enable iamcredentials.googleapis.com \
    bigquery.googleapis.com \
    storage-api.googleapis.com \
    logging.googleapis.com

# 3. Workload Identity Pool作成
log_info "Workload Identity Pool作成中..."
if gcloud iam workload-identity-pools describe $POOL_ID --location=global &>/dev/null; then
    log_warning "Workload Identity Pool '$POOL_ID' は既に存在します"
else
    gcloud iam workload-identity-pools create $POOL_ID \
        --project=$PROJECT_ID \
        --location=global \
        --display-name="GitHub Actions Pool" \
        --description="Workload Identity Pool for GitHub Actions"
    log_success "Workload Identity Pool '$POOL_ID' を作成しました"
fi

# 4. OIDC Provider作成
log_info "OIDC Provider作成中..."
if gcloud iam workload-identity-pools providers describe $PROVIDER_ID \
    --location=global \
    --workload-identity-pool=$POOL_ID &>/dev/null; then
    log_warning "OIDC Provider '$PROVIDER_ID' は既に存在します"
else
    gcloud iam workload-identity-pools providers create-oidc $PROVIDER_ID \
        --project=$PROJECT_ID \
        --location=global \
        --workload-identity-pool=$POOL_ID \
        --display-name="GitHub Provider" \
        --description="OIDC Provider for GitHub Actions" \
        --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.ref=assertion.ref" \
        --issuer-uri="https://token.actions.githubusercontent.com"
    log_success "OIDC Provider '$PROVIDER_ID' を作成しました"
fi

# 5. サービスアカウント作成（最小権限）
log_info "サービスアカウント作成中..."
if gcloud iam service-accounts describe $SA_EMAIL &>/dev/null; then
    log_warning "サービスアカウント '$SA_EMAIL' は既に存在します"
else
    gcloud iam service-accounts create $SA_NAME \
        --project=$PROJECT_ID \
        --display-name="GitHub Actions Data Pipeline" \
        --description="Minimal permissions service account for data pipeline"
    log_success "サービスアカウント '$SA_EMAIL' を作成しました"
fi

# 6. 最小権限の付与
log_info "最小権限を付与中..."

# BigQuery権限（カスタムロールの代わりに既存ロールを使用）
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.dataEditor" \
    --condition=None

# BigQueryジョブ実行権限
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.jobUser" \
    --condition=None

log_success "BigQuery権限を付与しました"

# 7. Workload Identity結合
log_info "Workload Identity結合中..."
MEMBER="principalSet://iam.googleapis.com/projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/attribute.repository/$GITHUB_REPO"

gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL \
    --project=$PROJECT_ID \
    --role="roles/iam.workloadIdentityUser" \
    --member="$MEMBER"

log_success "Workload Identity結合を完了しました"

# 8. 設定情報出力
log_info "GitHub Secrets設定情報を出力中..."

WIF_PROVIDER="projects/$PROJECT_NUMBER/locations/global/workloadIdentityPools/$POOL_ID/providers/$PROVIDER_ID"

echo ""
echo "=========================================="
echo " GitHub Repository Secrets 設定情報"
echo "=========================================="
echo ""
echo "以下の値をGitHub Repository Settings > Secrets and variables > Actions に設定してください："
echo ""
echo "GCP_PROJECT_ID=$PROJECT_ID"
echo "GCP_PROJECT_NUMBER=$PROJECT_NUMBER"
echo "WIF_PROVIDER=$WIF_PROVIDER"
echo "WIF_SERVICE_ACCOUNT=$SA_EMAIL"
echo ""
echo "=========================================="
echo " 追加で必要な設定"
echo "=========================================="
echo ""
echo "GCS_BUCKET=your-gcs-bucket-name"
echo "GCS_PREFIX=raw_data  # オプション"
echo ""

# 9. Storage権限設定の案内
echo "=========================================="
echo " Storage権限設定（手動実行が必要）"
echo "=========================================="
echo ""
echo "GCSバケットが作成済みの場合、以下コマンドでStorage権限を付与してください："
echo ""
echo "gsutil iam ch serviceAccount:$SA_EMAIL:roles/storage.objectAdmin gs://YOUR_BUCKET_NAME"
echo ""

# 10. 検証用スクリプト出力
cat > verify_auth.sh << EOF
#!/bin/bash
# 認証設定検証スクリプト
echo "認証設定を検証中..."

echo "1. Workload Identity Pool確認:"
gcloud iam workload-identity-pools describe $POOL_ID --location=global --format="table(name,displayName,state)"

echo "2. OIDC Provider確認:"
gcloud iam workload-identity-pools providers describe $PROVIDER_ID \\
    --location=global \\
    --workload-identity-pool=$POOL_ID \\
    --format="table(name,displayName,state)"

echo "3. サービスアカウント確認:"
gcloud iam service-accounts describe $SA_EMAIL --format="table(email,displayName,disabled)"

echo "4. サービスアカウント権限確認:"
gcloud projects get-iam-policy $PROJECT_ID \\
    --flatten="bindings[].members" \\
    --filter="bindings.members:serviceAccount:$SA_EMAIL" \\
    --format="table(bindings.role)"

echo "認証設定検証完了"
EOF

chmod +x verify_auth.sh

log_success "セキュアな認証設定が完了しました！"
log_info "設定検証: ./verify_auth.sh を実行してください"
log_info "次のステップ: GitHub Actionsワークフローを更新してください"

echo ""
echo "=========================================="
echo " 次のステップ"
echo "=========================================="
echo ""
echo "1. 上記のSecrets情報をGitHubに設定"
echo "2. GitHub Actionsワークフローを更新（Workload Identity使用）"
echo "3. ./verify_auth.sh で設定確認"
echo "4. テスト実行してWorkload Identity動作確認"
echo "5. 古いJSONキーファイルとSecretsを削除"
echo ""