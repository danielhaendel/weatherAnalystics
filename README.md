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

---

## Docker

> Voraussetzung: Docker Desktop (oder Docker Engine + Docker Compose v2) ist installiert und läuft.

1. **.env vorbereiten**
   ```bash
   cp .env.example .env
   ```
   Trage mindestens `GEOAPIFY_KEY=<dein_token>` ein. Optional kannst du `API_ACCESS_KEY=<token>` setzen, damit das Frontend sofort einen API-Key besitzt.

2. **Images bauen & Container starten**
   ```bash
   docker compose up --build
   ```
   - Der Web-Container veröffentlicht Port `5000`; die App ist danach unter [http://127.0.0.1:5000](http://127.0.0.1:5000) erreichbar.
   - Logs kannst du live verfolgen mit `docker compose logs -f web`.

3. **Im Hintergrund laufen lassen**
   ```bash
   docker compose up -d
   ```
   - Stoppen: `docker compose down`
   - Neu starten ohne Rebuild: `docker compose up -d web`

4. **Persistente Daten**
   - Die SQLite-Datenbank liegt im Container unter `/app/instance`.
   - Wenn du sie lokal behalten möchtest, kannst du im `docker-compose.yml` ein Bind-Mount für `./instance` aktivieren.

5. **Environment ändern**
   - Änderungen in `.env` oder am Code erfordern ein `docker compose up --build`, damit das Image neu gebaut wird.

Damit ist das komplette Backend über Docker nutzbar, inklusive Swagger-UI (`/docs`), Admin-Panel (`/admin`) und API-Key-geschützter Endpunkte.

---

## API-Zugriff & Swagger

Alle öffentlich dokumentierten Endpunkte (`/api/data/coverage`, `/api/stations/nearest`, `/api/stations_in_radius`, `/api/reports/aggregate`) verlangen einen gültigen API-Key:

* Für interne Views wird automatisch der in `.env` definierte `API_ACCESS_KEY` verwendet.
* Für externe Tools (Swagger, Bruno, etc.) bitte im Admin-Panel (Register > „API-Keys“) einen neuen Schlüssel erzeugen.
* Beim Aufruf im Header `X-API-Key: <dein_token>` mitsenden (alternativ als Query-Parameter `?api_key=<dein_token>`).
* Die Swagger-UI unter `/docs` nutzt dieselben Endpunkte und akzeptiert den Header ebenfalls.

## API-Test mit Bruno

Zur schnellen Überprüfung der REST-Endpunkte liegt eine Bruno-Collection unter `bruno/WeatherAnalytics` bereit:

1. Bruno installieren und starten.
2. Über **Open Collection** das Verzeichnis `bruno/WeatherAnalytics` auswählen.
3. Die vorkonfigurierte Umgebung `local` erwartet einen lokal laufenden Flask-Server (`http://localhost:5000`) und nutzt Beispielwerte für Berlin. Passe bei Bedarf die Variablen in `bruno/WeatherAnalytics/environments/local.bru` an.
4. Die Requests (Coverage, Stations in Radius, Nearest Station, Aggregate Report, Analyze) enthalten Beispielpayloads und können sofort abgesetzt werden, solange im Hintergrund der Flask-Server läuft.

## Extra Features
1. **Datenexport**: Exportiere analysierte Daten als XLSX
2. **API-Keys**: Sichere API-Zugriffe mit individuellen Schlüsseln
3. **Login/Register**: Admin- und API-Nutzer
4. **Multilinguale Oberfläche**: Deutsch, Englisch, Spanisch, Französisch und Polnisch
5. **Dark/Light Mode**: Umschaltbare UI-Themes
6. **SwaggerUI**: Interaktive API-Dokumentation unter `http://127.0.0.1:5000/docs` (inkl. API-Key Header)
7. **Docker-Support**: Einfache Bereitstellung via Docker Compose
