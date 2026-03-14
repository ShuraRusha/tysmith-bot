import urllib.request, os

BASE = os.path.dirname(os.path.abspath(__file__))

FONTS = {
    "Nunito-Regular.ttf": "https://github.com/google/fonts/raw/main/ofl/nunito/Nunito-Regular.ttf",
    "Nunito-Bold.ttf":    "https://github.com/google/fonts/raw/main/ofl/nunito/Nunito-Bold.ttf",
}

def ensure_fonts():
    for name, url in FONTS.items():
        path = os.path.join(BASE, name)
        if not os.path.exists(path):
            print(f"Downloading {name}...")
            urllib.request.urlretrieve(url, path)
            print(f"Saved {name}")
