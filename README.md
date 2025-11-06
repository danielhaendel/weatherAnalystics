# WeatherAnalytics

## Überblick

**WeatherAnalytics** sammelt und analysiert Wetterdaten, bereitet sie in einem Dashboard auf und ermöglicht das Aktualisieren der Daten über ein integriertes Admin-Panel. Fokus: schnelle Auswertung von Temperatur- und Niederschlagsreihen zur Erkennung von Trends und Anomalien.

---

## Voraussetzungen

* **Python 3.11** (oder kompatibel)
* **Git** (optional, falls das Repository geklont wird)

---

## Installation & Start (einzelne Befehle)

**Repository klonen**

```bash
git clone https://github.com/danielhaendel/weatherAnalystics
```

```bash
cd weatherAnalystics
```

**Virtuelle Umgebung erstellen**

```bash
python -m venv .venv
```

**Virtuelle Umgebung aktivieren (Windows PowerShell)**

```bash
.venv\Scripts\Activate.ps1
```

**Virtuelle Umgebung aktivieren (Windows Eingabeaufforderung / CMD)**

```bash
.venv\Scripts\activate.bat
```

**Virtuelle Umgebung aktivieren (Linux/macOS)**

```bash
source .venv/bin/activate
```

**Pip aktualisieren**

```bash
pip install --upgrade pip
```

**Abhängigkeiten installieren**

```bash
pip install -r requirements.txt
```

**Flask-Server starten**

```bash
flask --app weather_analytics.app run
```

Nach dem Start: **[http://127.0.0.1:5000/](http://127.0.0.1:5000/)**

---

## Admin-Panel

* URL: **[http://127.0.0.1:5000/admin](http://127.0.0.1:5000/admin)**
* Standard-Zugangsdaten:

  * Benutzername: `admin`
  * Passwort: `admin`

**Aktualisieren im Admin-Panel**

1. **Admin-Panel** öffnen
2. **Daten aktualisieren**
3. Stationsdaten oder Wetterdaten auswählen
4. Auf **Starten** klicken
5. Warten, bis der Vorgang abgeschlossen ist (Wetterdaten können einige Minuten dauern)

---

## Entwicklung

**.env aus Beispiel erstellen**

```bash
cp .env.example .env
```

**Tests ausführen**

```bash
pytest -q
```
