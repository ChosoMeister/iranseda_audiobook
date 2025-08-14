# 📚 IranSeda Audiobook Scraper

**IranSeda Audiobook Scraper** یک ابزار خط فرمان (CLI) برای جمع‌آوری و پردازش اطلاعات کتاب‌های صوتی وب‌سایت [book.iranseda.ir](https://book.iranseda.ir) است.  
این اسکریپر صفحات لیست کتاب‌ها را پیمایش کرده، جزئیات هر کتاب را استخراج می‌کند، لینک‌های پخش و دانلود MP3 را از API رسمی می‌گیرد و خروجی نهایی را در قالب CSV ذخیره می‌کند.

---

## ✨ ویژگی‌ها

- **خزش (Crawling)** صفحات لیست کتاب‌ها (صفحه ۱ تا N) و جمع‌آوری شناسه‌ها و لینک‌ها
- **پردازش (Enriching)** و استخراج متادیتا از صفحه جزئیات کتاب
- **دریافت لینک پخش و MP3** از API رسمی بدون واسطه CSV
- ذخیره خروجی در قالب **UTF-8-SIG** (سازگار با Excel فارسی)
- پشتیبانی از **Throttle تصادفی** برای جلوگیری از بلاک شدن IP
- **Atomic CSV Write** برای ادامه کار ایمن در صورت توقف

---

## 📦 نصب و راه‌اندازی

```bash
# ایجاد محیط مجازی
python -m venv .venv

# فعال‌سازی محیط مجازی
source .venv/bin/activate        # روی لینوکس/مک
.venv\Scripts\activate           # روی ویندوز

# نصب وابستگی‌ها
pip install -r requirements.txt
```

---

## 🚀 نحوه استفاده

### اجرای کامل با فایل کانفیگ YAML
```bash
python -m iranseda.cli run --config configs/config.yaml
```

### اجرای مرحله‌ای (اختیاری - حالت قدیمی)
```bash
# مرحله ۱: خزش لیست کتاب‌ها
python -m iranseda.cli crawl --config configs/config.yaml

# مرحله ۲: پردازش جزئیات و دانلود لینک‌ها
python -m iranseda.cli enrich --config configs/config.yaml
```

> **نکته:** دستور `run` به صورت پیش‌فرض هر دو مرحله را بدون نیاز به فایل میانی انجام می‌دهد.

---

## ⚙️ ساختار پروژه

```
iranseda_audiobook/
│
├── configs/                # فایل‌های پیکربندی YAML
├── output/                 # خروجی‌ها (CSV و داده‌های پردازش‌شده)
├── scripts/                # اسکریپت‌های شل برای اجرای سریع
├── src/iranseda/           # کد اصلی پروژه
│   ├── cli.py               # رابط خط فرمان
│   ├── listing.py           # ماژول خزش صفحات
│   ├── details.py           # ماژول پردازش جزئیات
│   ├── pipeline.py          # اجرای یکپارچه
│   ├── utils.py             # توابع کمکی
│   └── config.py            # بارگذاری تنظیمات
├── tests/                  # تست‌های واحد
├── requirements.txt        # وابستگی‌های پایتون
├── pyproject.toml           # تنظیمات پکیج
└── README.md
```

---

## 📄 قالب خروجی CSV

خروجی به صورت UTF-8-SIG ذخیره می‌شود و شامل ستون‌های زیر است:

| ستون | توضیح |
|------|-------|
| Title | عنوان کتاب |
| Author | نویسنده |
| Narrator | گوینده |
| Duration | مدت زمان |
| Cover_Image_URL | لینک تصویر جلد |
| Source_URL | لینک صفحه کتاب |
| Player_Link | لینک پخش آنلاین |
| FullBook_MP3_URL | لینک دانلود کامل |
| All_MP3s_Found | همه لینک‌های MP3 |

---

## 🧪 اجرای تست‌ها

```bash
pytest tests/
```

---

## 🛠 وابستگی‌ها

- Python 3.10+
- Requests
- PyYAML
- BeautifulSoup4

---

## 📜 لایسنس

این پروژه تحت مجوز **MIT** منتشر شده است.  
آزادید از آن استفاده و تغییر دهید.
