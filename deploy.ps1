# نشر فلتر الانفراج RSI على Koyeb (مجاني، يعمل 24/7 بدون جهازك)
# الخطوات:
#   1) سجّل حساباً مجانياً: https://app.koyeb.com/signup
#   2) أنشئ API token من:   https://app.koyeb.com/account/api  (زر Create API token)
#   3) في هذا الطرفية ضع التوكن:
#        $env:KOYEB_TOKEN = "ضع_التوكن_هنا"
#   4) نفّذ:  powershell -ExecutionPolicy Bypass -File .\deploy.ps1
$ErrorActionPreference = "Stop"
$bin = Join-Path $env:USERPROFILE ".koyeb\bin\koyeb.exe"
if (-not (Test-Path $bin)) { Write-Host "Koyeb CLI غير مثبت." -ForegroundColor Red; exit 1 }
if (-not $env:KOYEB_TOKEN) {
    Write-Host "ضع التوكن أولاً:" -ForegroundColor Yellow
    Write-Host '  $env:KOYEB_TOKEN = "توكنك"'
    exit 1
}

& $bin deploy . "rsi-scanner/web" `
  --archive-builder docker `
  --ports "5000:http" `
  --routes "/:5000" `
  --checks "5000:http:/api/health" `
  --instance-type micro `
  --archive-ignore-dir ".git" `
  --archive-ignore-dir ".venv" `
  --archive-ignore-dir "__pycache__"
