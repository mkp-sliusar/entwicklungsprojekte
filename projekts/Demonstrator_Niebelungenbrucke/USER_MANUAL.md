# Benutzerhandbuch Nibelung Bridge

Dieses Handbuch erklärt, wie Sie das Gerät einschalten, eine Verbindung mit
seinem WLAN-Zugangspunkt herstellen, die Weboberfläche öffnen, eine Tarierung
durchführen und den Akkustand prüfen. Technische Einstellungen für die
Wartung des Systems sind in der [README.md](README.md) beschrieben.

## 1. Vor dem Start

Vor dem Einschalten:

- Stellen Sie das Gerät auf eine stabile Oberfläche.
- Vergewissern Sie sich, dass der Messbereich unbelastet ist und sich nicht
  bewegt.
- Wenn das Gerät längere Zeit nicht verwendet wurde, schließen Sie ein
  geeignetes Ladegerät am USB-C-Anschluss an.
- Halten Sie einen Computer mit WLAN bereit.

Nach dem Einschalten benötigt das System ungefähr **eine Minute**, um alle
Dienste zu starten und den WLAN-Zugangspunkt bereitzustellen.

> **Wichtig:** Bewegen Sie das Gerät während des Startvorgangs nicht. Die
> Sensoren werden nach dem Start automatisch tariert.

## 2. Tasten, Anschluss und LEDs

Alle Bedienelemente befinden sich an der Seitenwand.

| Element | Funktion |
| --- | --- |
| Einschalttaste | Gerät einschalten, Akkustand anzeigen und Gerät ausschalten. |
| Sensortaste für die Tarierung | Tarierung vorübergehend deaktivieren und eine neue Tarierung starten. |
| USB-C-Anschluss | Akku laden. |
| Erste blaue LED | Zeigt an, dass das Gerät eingeschaltet ist. |
| Die folgenden vier LEDs | Zeigen ungefähr den Akkustand an: Je mehr LEDs leuchten, desto höher ist der Ladezustand. |

**Platz für Foto 1:** Gesamtansicht des Geräts mit markierten Tasten, dem
USB-C-Anschluss und den LEDs.

**Platz für Foto 2:** Detailansicht der Seitenwand.

## 3. Einschalten und Verbinden

### 3.1. Einschalten

1. Drücken Sie die Einschalttaste **zweimal schnell hintereinander**.
2. Halten Sie die Taste während des zweiten Drückens ungefähr **5-6 Sekunden**
   gedrückt.
3. Lassen Sie die Taste los, sobald das Gerät startet. Die erste blaue LED muss
   anzeigen, dass die Stromversorgung eingeschaltet ist.
4. Warten Sie ungefähr **eine Minute**. In dieser Zeit werden die internen
   Dienste und der WLAN-Zugangspunkt gestartet.

### 3.2. Computer mit dem WLAN verbinden

1. Öffnen Sie am Computer die Liste der verfügbaren WLAN-Netzwerke.
2. Wählen Sie das folgende Netzwerk aus:

   - **Name des Zugangspunkts:** `Demonstrator-Nibelungenbruecke`
   - **Passwort:** `Nibelungen2026`

3. Warten Sie auf die Meldung, dass die Verbindung erfolgreich hergestellt
   wurde.

Nach der Verbindung kann der Computer auf das Gerät zugreifen. Wenn das Gerät
über eine externe Internetverbindung verfügt, steht dem Computer über dasselbe
WLAN auch der Internetzugang zur Verfügung. Ohne externe Internetverbindung
bleibt der lokale Zugriff auf das Gerät und seine Dashboards trotzdem möglich.

**Platz für Foto 3:** Auswahl des Zugangspunkts am Computer.

### 3.3. Hauptwebsite öffnen

Öffnen Sie nach der Verbindung einen Browser und rufen Sie folgende Adresse auf:

[https://digitaler-zwilling.dev.marxkrontal.com/](https://digitaler-zwilling.dev.marxkrontal.com/)

Weitere Anweisungen zur Nutzung der Website werden ergänzt, sobald sie von der
zuständigen Abteilung bereitgestellt wurden.

**Platz für Foto 4:** Startseite der Website nach erfolgreicher Verbindung.

**Platz für Foto 5:** Beispielansicht mit Messwerten.

## 4. Messwerte tarieren

### 4.1. Automatische Tarierung

Die Tarierung wird beim Systemstart automatisch gestartet. Für ein korrektes
Ergebnis muss das Gerät zu diesem Zeitpunkt ruhig stehen und der Messbereich
unbelastet sein.

### 4.2. Manuelle Tarierung

Die Sensortaste für die Tarierung befindet sich an der Seitenwand.

1. **Ein kurzer Druck:** Die Tarierung wird deaktiviert und das System zeigt
   die Rohwerte der Messungen an.
2. **Ein weiterer kurzer Druck:** Eine neue Tarierung wird gestartet. Danach
   werden wieder korrigierte Messwerte verwendet.

Berühren oder belasten Sie das Gerät während der erneuten Tarierung nicht. Wenn
die Messwerte danach nicht korrekt aussehen, wiederholen Sie den Vorgang bei
ruhigem, unbelastetem Gerät.

**Platz für Foto 6:** Sensortaste für die Tarierung mit markierter
Berührungsfläche.

## 5. Laden und Akkuanzeige

Schließen Sie zum Laden ein USB-C-Kabel am Anschluss an der Seitenwand an.

- Die erwartete Akkulaufzeit beträgt **8-10 Stunden**. Die tatsächliche Laufzeit
  hängt von der Auslastung, der WLAN-Aktivität, den Sensoren und der Temperatur
  ab.
- Die erste blaue LED zeigt an, dass das Gerät eingeschaltet ist.
- Die folgenden vier LEDs zeigen den Akkustand an. Ein einzelner kurzer Druck
  auf die Einschalttaste zeigt den aktuellen Ladezustand an.
- Durch langes Drücken der Einschalttaste wird das Gerät ausgeschaltet.

Die Anzeige des Akkustands auf der Hauptwebsite ist derzeit noch nicht
implementiert. Bis dahin kann der Ladezustand über die LEDs an der Seitenwand
oder über die lokale Überwachung geprüft werden, sofern ein entsprechender
Batteriesensor im System verfügbar ist.

**Platz für Foto 7:** Beispiele für verschiedene Ladezustände.

## 6. Lokale Überwachung für erfahrene Benutzer

Die lokale Überwachung ist nützlich, wenn die Hauptwebsite oder die externe
Internetverbindung nicht verfügbar ist. Verbinden Sie sich zuerst wie in
Abschnitt 3 beschrieben mit dem WLAN des Geräts.

Öffnen Sie im Browser:

[http://192.168.4.1:3000](http://192.168.4.1:3000)

Standard-Anmeldedaten:

- **Benutzername:** `admin`
- **Passwort:** `admin`

In der lokalen Grafana-Instanz stehen Dashboards für verschiedene Aufgaben zur
Verfügung:

- **Live data** - Aktuelle Messwerte und Tarierungsstatus.
- **Historische Daten** - Historische Werte und Diagramme.
- **Diagnostik** - Diagnoseinformationen und Messstatus.
- **System Monitor** - Systemstatus, Temperatur, freier Speicherplatz,
  LTE- oder andere Netzwerkverbindung sowie Akkudaten, sofern diese vom
  angeschlossenen Sensor bereitgestellt werden.

> **Sicherheit:** `admin/admin` sind die Standard-Anmeldedaten. Geben Sie sie
> nicht an Dritte weiter und ändern Sie das Passwort, wenn Grafana dies anbietet.

**Platz für Foto 8:** Anmeldeseite der lokalen Grafana-Instanz.

**Platz für Foto 9:** Beispiel des Dashboards Live data.

## 7. Fehlerbehebung

| Problem | Was ist zu prüfen? |
| --- | --- |
| Das Gerät schaltet sich nicht ein. | Prüfen Sie den Ladezustand und das USB-C-Kabel. Drücken Sie die Taste erneut zweimal schnell und halten Sie sie während des zweiten Drückens 5-6 Sekunden gedrückt. |
| Der Zugangspunkt wird nicht angezeigt. | Stellen Sie sicher, dass die blaue LED leuchtet. Warten Sie eine vollständige Minute und aktualisieren Sie die WLAN-Liste am Computer. |
| WLAN ist verbunden, aber die Website öffnet sich nicht. | Prüfen Sie die Website-Adresse. Öffnen Sie zur lokalen Prüfung `http://192.168.4.1:3000`. |
| Nach der WLAN-Verbindung gibt es keinen Internetzugang. | Der lokale Zugriff kann auch ohne externes Internet funktionieren. Prüfen Sie die externe Verbindung, über die das Gerät Internetzugang erhalten soll. |
| Die Messwerte sind instabil oder falsch. | Stellen Sie sicher, dass das Gerät ruhig und unbelastet ist, und führen Sie anschließend eine manuelle Tarierung durch. |
| Die Messwerte sind nach der Tarierung weiterhin falsch. | Wiederholen Sie die Tarierung ohne Belastung. Bewegen Sie das Gerät nicht, bis der Vorgang abgeschlossen ist. |
| Die lokale Grafana-Instanz öffnet sich nicht. | Stellen Sie sicher, dass der Computer mit dem Zugangspunkt des Geräts verbunden ist, und warten Sie, bis die Dienste vollständig gestartet sind. |
| Der Akkustand wird auf der Website nicht angezeigt. | Dies ist eine Einschränkung der aktuellen Version: Die Akkuanzeige auf der Hauptwebsite ist noch nicht implementiert. Prüfen Sie die LEDs oder die lokale Überwachung. |

Wenn das Problem nach diesen Prüfungen weiterhin besteht, notieren Sie den
Zustand der LEDs, den Zeitpunkt des Problems und alle Meldungen auf dem
Bildschirm. Geben Sie diese Informationen anschließend an die zuständige
Person weiter.

## 8. Ausschalten und Aufbewahrung

Halten Sie zum Ausschalten die Einschalttaste gedrückt, bis sich das Gerät
ausschaltet. Ziehen Sie vor dem Transport das USB-C-Kabel ab und vergewissern
Sie sich, dass alle LEDs erloschen sind.

Bewahren Sie das Gerät an einem trockenen Ort auf und lassen Sie es nicht über
längere Zeit vollständig entladen.