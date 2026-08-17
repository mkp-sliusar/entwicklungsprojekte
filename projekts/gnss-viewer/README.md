# GNSS Viewer

Der GNSS Viewer ist eine Flask-/Leaflet-Webanwendung zur Darstellung von
aktuellen und historischen GNSS-Positionen aus PostgreSQL. Zusätzlich können
geometrische Brückenachsen als eigener Karten-Layer eingeblendet werden.

Die Anwendung unterstützt mehrere Brückenprojekte. Das Standardprojekt und
die zugehörigen Geometriedateien werden in `bridge_projects.json` festgelegt.
Der Achsen-Layer wird im UI über die jeweilige Checkbox ein- und ausgeblendet;
die Achsen werden getrennt von den GNSS-Fahrzeugspuren über `/api/axes`
geladen.

## Funktionen

- Live-Karte mit regelmäßiger Aktualisierung der GNSS-Positionen
- Historische GNSS-Daten aus PostgreSQL
- Auswahl mehrerer Brückenprojekte
- Ein- und ausblendbarer Achsen-Layer mit Achsenbeschriftungen
- GeoJSON-basierte, geodätisch bestimmte Achsen
- Stationsauswertung entlang stationierter Achsen
- Healthcheck für die PostgreSQL-Verbindung

## Projektübersicht

Die mitgelieferte `bridge_projects.json` enthält aktuell diese Projekte:

| ID | Projekt | Achsenquelle |
|---|---|---|
| `bestand_a_l` | Dresden, Budapester Straße, Achsen A–L | `axes.geojson` |
| `waren_herrenseebruecke` | Waren, Herrenseebrücke, Achsen 0–13 | `waren_herrenseebruecke_axes.geojson` |

Für jedes Projekt werden unter `axes` mindestens `mode` und bei GeoJSON-
Projekten zusätzlich `path` angegeben. Unterstützte Achsmodi sind:

- `geojson`: Die Achsen werden aus einer GeoJSON-Datei gelesen.
- `circular_arc`: Die Achsen werden aus Kreisbogen, Endpunkten und
  Stationswerten erzeugt.

Das Feld `default_project` legt das beim Aufruf der Anwendung zunächst
ausgewählte Projekt fest. Relative Pfade in `bridge_projects.json` beziehen
sich auf das Verzeichnis dieser Datei.

## Achsen A–L aus dem Plan

Im Projekt `bestand_a_l` werden die Achsen A, B, C, D, E, F, G, H, J, K und L
verwendet. Die Bezeichnung **I** wird entsprechend dem zugrunde liegenden Plan
übersprungen.

| Abschnitt | Abstand (m) | Station (m) |
|---|---:|---:|
| A–B | 30,0 | 30,0 |
| B–C | 31,5 | 61,5 |
| C–D | 37,0 | 98,5 |
| D–E | 33,5 | 132,0 |
| E–F | 33,5 | 165,5 |
| F–G | 33,5 | 199,0 |
| G–H | 33,5 | 232,5 |
| H–J | 38,5 | 271,0 |
| J–K | 35,5 | 306,5 |
| K–L | 17,0 | 323,5 |

Die Gesamtstation von A bis L beträgt damit `323,5 m`.

## Voraussetzungen

- Docker mit Docker-Compose-Plugin oder Python 3.12+ für einen manuellen
  Betrieb
- Netzwerkzugriff der Anwendung auf einen PostgreSQL-Server
- Eine Datenbank mit der vom Server erwarteten Tabelle
  `public."GNSS_Position_Belastungsfahrzeug"`
- Gültige Zugangsdaten in `gnss-viewer.env`

## Start mit Docker Compose

1. In das Projektverzeichnis wechseln:

   ```bash
   cd /home/user/gnss-viewer
   ```

2. Konfigurationsdatei anlegen:

   ```bash
   cp gnss-viewer.env.example gnss-viewer.env
   ```

3. In `gnss-viewer.env` mindestens PostgreSQL-Host, Datenbank, Benutzer und
   Passwort anpassen. Die Datei enthält vertrauliche Zugangsdaten und darf
   nicht veröffentlicht oder eingecheckt werden.

4. Image bauen und Dienst starten:

   ```bash
   docker compose up -d --build
   ```

5. Die Anwendung öffnen:

   ```text
   http://localhost:8050
   ```

Bei Zugriff von einem anderen Rechner ist `localhost` durch den Hostnamen oder
die IP-Adresse des Servers zu ersetzen. Der Container lauscht standardmäßig
auf Port `8050`.

Nützliche Docker-Befehle:

```bash
docker compose ps
docker compose logs -f gnss-viewer
docker compose restart gnss-viewer
docker compose down
```

Nach Änderungen an `bridge_projects.json` oder an einer beim Image-Bau
kopierten GeoJSON-Datei das Image erneut bauen:

```bash
docker compose up -d --build
```

## Konfiguration

Die Vorlage steht in `gnss-viewer.env.example`. Die wichtigsten Variablen sind:

| Variable | Bedeutung | Standard |
|---|---|---|
| `GNSS_DB_HOST` | Hostname oder IP des PostgreSQL-Servers | `10.20.38.240` |
| `GNSS_DB_PORT` | PostgreSQL-Port | `5432` |
| `GNSS_DB_NAME` | Datenbankname | `sensors_db` |
| `GNSS_DB_USER` | Datenbankbenutzer | `iris_user` |
| `GNSS_DB_PASSWORD` | Datenbankpasswort | leer |
| `GNSS_APP_HOST` | Bind-Adresse der Webanwendung | `0.0.0.0` |
| `GNSS_APP_PORT` | Port der Webanwendung | `8050` |
| `GNSS_APP_TITLE` | Seitentitel | `GNSS Live & Historie` |
| `GNSS_LIVE_MINUTES` | Zeitraum für die Live-Ansicht | `5` |
| `GNSS_LIVE_REFRESH_MS` | Aktualisierungsintervall im Browser | `1000` |
| `GNSS_MAX_HISTORY_HOURS` | Maximales Zeitfenster der Historie | `48` |
| `GNSS_MAX_POINTS_PER_VEHICLE` | Maximale Punkte je Fahrzeug | `12000` |
| `GNSS_MAX_INCREMENTAL_ROWS` | Maximale inkrementelle Datenzeilen | `5000` |
| `GNSS_AXES_VISIBLE` | Anfangszustand des Achsen-Layers | `true` |
| `GNSS_PROJECTS_CONFIG` | Pfad zur Projektdatei | `bridge_projects.json` |

Die Achsenquelle wird bei den mitgelieferten Projekten in
`bridge_projects.json` festgelegt. `GNSS_AXES_GEOJSON` dient als Standardpfad,
falls ein GeoJSON-Projekt keinen eigenen `axes.path`-Wert besitzt. Es ersetzt
nicht automatisch die im Projekt eingetragene Datei.

Die Variablen `GNSS_AXIS_A_LAT`, `GNSS_AXIS_A_LON`, `GNSS_AXIS_L_LAT`,
`GNSS_AXIS_L_LON` und `GNSS_AXES_WIDTH_M` gehören zur älteren Logik für aus zwei
Referenzpunkten erzeugte gerade Achsen. Für die aktuelle Mehrprojekt-
Konfiguration sind sie nicht erforderlich; die mitgelieferten Projekte nutzen
konfigurierte GeoJSON-Dateien.

## GeoJSON-Achsen

Eine GeoJSON-Datei muss eine `FeatureCollection` mit `LineString`-Features
enthalten. Jedes Feature braucht mindestens die Eigenschaft `axis` und zwei
Koordinatenpunkte. Die Koordinaten sind WGS84 und werden in GeoJSON-Reihenfolge
angegeben: **Längengrad, Breitengrad**.

Beispiel:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "axis": "A",
        "station_m": 0.0,
        "label_coordinates": [13.0001, 51.0001]
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [13.00000000, 51.00000000],
          [13.00010000, 51.00010000]
        ]
      }
    }
  ]
}
```

`station_m` ist die Planstation der Querachse. Für eine stationierte
Auswertung, beispielsweise beim Projekt `waren_herrenseebruecke`, müssen die
Stationen eindeutig und streng aufsteigend sein. `label_coordinates` ist
optional und legt die Position der Achsenbeschriftung fest.

Die Dateien `axes.geojson` und `waren_herrenseebruecke_axes.geojson` enthalten
bereits die derzeit verwendeten Geometrien. Platzhalterkoordinaten sollten
nicht produktiv verwendet werden. Für eine neue Brücke die geodätisch
bestimmten WGS84-Koordinaten in einer eigenen Datei hinterlegen und den Pfad in
`bridge_projects.json` eintragen.

## HTTP-Endpunkte

| Endpunkt | Zweck |
|---|---|
| `GET /` | Weboberfläche |
| `GET /health` | Prüft die Verbindung zur PostgreSQL-Datenbank |
| `GET /api/projects` | Liefert Projekte, Standardprojekt und Karteneinstellungen |
| `GET /api/axes?project=bestand_a_l` | Liefert die Achsen als GeoJSON |
| `GET /api/gnss` | Liefert GNSS-Daten für die Kartenansicht |

Beispiele:

```bash
curl http://localhost:8050/health
curl http://localhost:8050/api/projects
curl 'http://localhost:8050/api/axes?project=bestand_a_l'
```

`/health` antwortet bei erreichbarer Datenbank mit HTTP 200 und
`{"status":"ok", ...}`. Bei einem Datenbankfehler wird HTTP 503 geliefert.
`/api/axes` enthält unter anderem `project_id`, `source`, `configured` und die
GeoJSON-Features. Ein unbekanntes oder fehlerhaft konfiguriertes Projekt führt
zu einer Fehlermeldung.

## Systemd-Betrieb

Die Datei `gnss-viewer.service` ist auf folgende Installation zugeschnitten:

- Arbeitsverzeichnis: `/home/user/gnss-viewer`
- virtuelle Umgebung: `/home/user/gnss-viewer/venv`
- Konfiguration: `/home/user/gnss-viewer/gnss-viewer.env`

Vor der ersten Aktivierung müssen Abhängigkeiten installiert und die
Konfigurationsdatei angelegt werden:

```bash
cd /home/user/gnss-viewer
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp gnss-viewer.env.example gnss-viewer.env
# Danach gnss-viewer.env anpassen.
```

Die Laufzeitdateien müssen gemeinsam im Arbeitsverzeichnis liegen, insbesondere
`gnss_postgres_server.py`, `bridge_projects.json`, `axes.geojson` und
`waren_herrenseebruecke_axes.geojson`.

Danach Unit-Datei installieren und Dienst starten:

```bash
sudo cp gnss-viewer.service /etc/systemd/system/gnss-viewer.service
sudo systemctl daemon-reload
sudo systemctl enable --now gnss-viewer
sudo systemctl status gnss-viewer
```

Bei späteren Updates:

```bash
sudo systemctl restart gnss-viewer
sudo journalctl -u gnss-viewer -f
```

Wenn Benutzer oder Pfade abweichen, müssen `User`, `Group`,
`WorkingDirectory`, `EnvironmentFile` und `ExecStart` in der Unit-Datei
angepasst werden.

## Tests

Die Geometrie-Unit-Tests benötigen keine laufende PostgreSQL-Datenbank:

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

## Fehlersuche

**Die Anwendung startet, aber der Healthcheck liefert HTTP 503**

PostgreSQL-Host, Port, Datenbankname, Benutzer und Passwort in
`gnss-viewer.env` prüfen. Zusätzlich muss der Datenbankserver Verbindungen vom
Anwendungsserver zulassen. Die Container-Logs helfen bei der Ursachenanalyse:

```bash
docker compose logs --tail=100 gnss-viewer
```

**Der Achsen-Layer bleibt leer oder erscheint nicht**

Zuerst die projektbezogene Antwort prüfen:

```bash
curl 'http://localhost:8050/api/axes?project=bestand_a_l'
```

Danach `default_project`, `axes.mode`, `axes.path` und die GeoJSON-Struktur
kontrollieren. Bei relativen Pfaden muss die Datei neben
`bridge_projects.json` liegen. Ein GeoJSON-Feature muss ein gültiges
`LineString` mit mindestens zwei WGS84-Koordinaten enthalten.

**Änderungen an Projekt- oder Achsendateien werden nicht übernommen**

Das Docker-Image enthält `bridge_projects.json` und die GeoJSON-Dateien. Nach
Änderungen das Image neu bauen und den Dienst neu erstellen:

```bash
docker compose up -d --build
```

Die Datei `gnss-viewer.env` enthält echte Zugangsdaten und gehört nicht in ein
öffentliches Repository.
