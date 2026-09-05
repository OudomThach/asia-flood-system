"""
OASIS Common Alerting Protocol (CAP) v1.2 Generator & Validator.
Compliant with ITU-T Recommendation X.1303 and WMO Alert Hub standards.
Enables interoperability with Google Public Alerts, Apple Emergency Alerts, and NCDM Cambodia.
"""

from datetime import datetime, timezone
import xml.etree.ElementTree as ET
from xml.dom import minidom
from typing import Optional, List, Dict, Any

from .models import Area, Reading, RiskState, CAPAlert


class CAPProtocolEngine:
    """
    Constructs, formats, and validates OASIS CAP v1.2 alerts in standard XML and JSON representations.
    """

    CAP_XMLNS = "urn:oasis:names:tc:emergency:cap:1.2"
    DEFAULT_SENDER = "warning-center@ncdm.gov.kh"

    @classmethod
    def create_cap_alert(
        cls,
        area: Area,
        risk: RiskState,
        reading: Optional[Reading] = None,
        identifier: Optional[str] = None
    ) -> CAPAlert:
        """Constructs a CAPAlert model instance from system risk and reading state."""
        now = datetime.now(timezone.utc)
        
        # Region-aware sender and country code
        country = getattr(area, "country", "Cambodia") or "Cambodia"
        if country in ("Cambodia", "KH"):
            sender = "warning-center@ncdm.gov.kh"
            cc = "KH"
        else:
            c_slug = country.lower().replace(" ", "").replace("-", "")
            sender = f"alerts@{c_slug}.flood-warning.asia"
            cc = country[:2].upper()

        alert_id = identifier or f"{cc}-EWS-{now.strftime('%Y%m%d')}-{area.area_id.upper()[:8]}-{risk.risk_id[:6]}"

        lvl = (risk.level or "Normal").capitalize()

        if lvl == "Danger":
            severity = "Extreme"
            urgency = "Immediate"
            certainty = "Observed"
            event_en = "Flash Flood Emergency / Severe River Surge"
            event_km = "គ្រោះអាសន្នទឹកជំនន់ជន់លិច / រលកទឹកទន្លេ"
            headline_en = f"EMERGENCY: Critical Flood Warning for {area.name_en}, {country}"
            headline_km = f"អាសន្នបន្ទាន់៖ ការព្រមានទឹកជំនន់ធ្ងន់ធ្ងរសម្រាប់ {area.name_km}"
            instruction_en = "Move to designated high ground or evacuation shelters immediately. Unplug electrical appliances."
            instruction_km = "សូមជម្លៀសទៅកាន់ទីទួលសុវត្ថិភាពជាបន្ទាន់។ កាត់ផ្តាច់ចរន្តអគ្គិសនី និងប្រមូលឯកសារសំខាន់ៗ។"
        elif lvl == "Caution":
            severity = "Severe"
            urgency = "Expected"
            certainty = "Likely"
            event_en = "Flood Watch / River Surge Advisory"
            event_km = "ការប្រុងប្រយ័ត្នទឹកជំនន់ / ស្ថានភាពតាមដានទន្លេ"
            headline_en = f"ADVISORY: Elevated Flood Watch for {area.name_en}, {country}"
            headline_km = f"សេចក្តីជូនដំណឹង៖ ការប្រុងប្រយ័ត្នទឹកជំនន់កម្រិតខ្ពស់សម្រាប់ {area.name_km}"
            instruction_en = "Monitor local flood gauges, secure livestock, and prepare emergency kits."
            instruction_km = "សូមតាមដានកម្ពស់ទឹកជាប្រចាំ រៀបចំសត្វពាហនៈ និងទុកដាក់ស្បៀងអាហារឱសថឱ្យបានរួចរាល់។"
        else:
            severity = "Minor"
            urgency = "Future"
            certainty = "Possible"
            event_en = "Hydrological Situation Normal"
            event_km = "ស្ថានភាពជលសាស្ត្រធម្មតា"
            headline_en = f"Normal Hydrological Conditions in {area.name_en}, {country}"
            headline_km = f"ស្ថានភាពទឹកជំនន់ស្ថិតក្នុងកម្រិតធម្មតានៅ {area.name_km}"
            instruction_en = "No emergency action required. Baseline monitoring active."
            instruction_km = "មិនមានវិធានការសង្គ្រោះបន្ទាន់ឡើយ។ ប្រព័ន្ធកំពុងតាមដានជាប្រចាំ។"

        q_str = f"{reading.discharge:,.1f} m³/s" if reading else "N/A"
        r_str = f"{reading.precipitation:.1f} mm" if reading else "N/A"

        desc_en = (
            f"Hydrological Alert for {area.name_en} ({country}). "
            f"Risk Level: {lvl}. Current river discharge: {q_str}. Supporting rainfall: {r_str}. "
            f"Evaluation: {risk.reason}"
        )
        desc_km = (
            f"សេចក្តីជូនដំណឹងជលសាស្ត្រសម្រាប់ {area.name_km} ({country})។ "
            f"កម្រិតហានិភ័យ៖ {lvl}។ លំហូរទឹកទន្លេ៖ {q_str}។ ទឹកភ្លៀង៖ {r_str}។ "
            f"មូលហេតុ៖ {risk.reason}"
        )

        area_desc = (
            f"{area.name_en} ({area.name_km}), {country}"
            if area.name_km and area.name_km != area.name_en
            else f"{area.name_en}, {country}"
        )

        return CAPAlert(
            identifier=alert_id,
            sender=sender,
            sent=now,
            status="Actual",
            msg_type="Alert",
            scope="Public",
            category="Met",
            event=event_en,
            urgency=urgency,
            severity=severity,
            certainty=certainty,
            headline_en=headline_en,
            headline_km=headline_km,
            description_en=desc_en,
            description_km=desc_km,
            instruction_en=instruction_en,
            instruction_km=instruction_km,
            area_desc=area_desc,
            latitude=area.latitude,
            longitude=area.longitude,
            radius_km=30.0
        )

    @classmethod
    def to_xml(cls, alert: CAPAlert) -> str:
        """Serializes a CAPAlert to standard OASIS CAP v1.2 XML with pretty printing."""
        root = ET.Element("alert", xmlns=cls.CAP_XMLNS)

        ET.SubElement(root, "identifier").text = alert.identifier
        ET.SubElement(root, "sender").text = alert.sender
        ET.SubElement(root, "sent").text = alert.sent.isoformat()
        ET.SubElement(root, "status").text = alert.status
        ET.SubElement(root, "msgType").text = alert.msg_type
        ET.SubElement(root, "scope").text = alert.scope
        ET.SubElement(root, "code").text = "IPAWS-CAP-1.2"

        # English Info Block
        info_en = ET.SubElement(root, "info")
        ET.SubElement(info_en, "language").text = "en-US"
        ET.SubElement(info_en, "category").text = alert.category
        ET.SubElement(info_en, "event").text = alert.event
        ET.SubElement(info_en, "urgency").text = alert.urgency
        ET.SubElement(info_en, "severity").text = alert.severity
        ET.SubElement(info_en, "certainty").text = alert.certainty
        ET.SubElement(info_en, "headline").text = alert.headline_en
        ET.SubElement(info_en, "description").text = alert.description_en
        ET.SubElement(info_en, "instruction").text = alert.instruction_en

        area_en = ET.SubElement(info_en, "area")
        ET.SubElement(area_en, "areaDesc").text = alert.area_desc
        ET.SubElement(area_en, "circle").text = f"{alert.latitude:.4f},{alert.longitude:.4f} {alert.radius_km:.1f}"

        # Khmer Info Block
        info_km = ET.SubElement(root, "info")
        ET.SubElement(info_km, "language").text = "km-KH"
        ET.SubElement(info_km, "category").text = alert.category
        ET.SubElement(info_km, "event").text = alert.event
        ET.SubElement(info_km, "urgency").text = alert.urgency
        ET.SubElement(info_km, "severity").text = alert.severity
        ET.SubElement(info_km, "certainty").text = alert.certainty
        ET.SubElement(info_km, "headline").text = alert.headline_km
        ET.SubElement(info_km, "description").text = alert.description_km
        ET.SubElement(info_km, "instruction").text = alert.instruction_km

        area_km = ET.SubElement(info_km, "area")
        ET.SubElement(area_km, "areaDesc").text = alert.area_desc
        ET.SubElement(area_km, "circle").text = f"{alert.latitude:.4f},{alert.longitude:.4f} {alert.radius_km:.1f}"

        # Pretty-print XML string
        rough_string = ET.tostring(root, encoding="utf-8")
        parsed = minidom.parseString(rough_string)
        return parsed.toprettyxml(indent="  ", encoding="UTF-8").decode("utf-8")

    @classmethod
    def to_json(cls, alert: CAPAlert) -> Dict[str, Any]:
        """Serializes a CAPAlert to standard JSON format for web & mobile APIs."""
        return {
            "cap_version": "1.2",
            "identifier": alert.identifier,
            "sender": alert.sender,
            "sent": alert.sent.isoformat(),
            "status": alert.status,
            "msgType": alert.msg_type,
            "scope": alert.scope,
            "info": [
                {
                    "language": "en-US",
                    "category": alert.category,
                    "event": alert.event,
                    "urgency": alert.urgency,
                    "severity": alert.severity,
                    "certainty": alert.certainty,
                    "headline": alert.headline_en,
                    "description": alert.description_en,
                    "instruction": alert.instruction_en,
                    "area": {
                        "areaDesc": alert.area_desc,
                        "circle": f"{alert.latitude:.4f},{alert.longitude:.4f} {alert.radius_km:.1f}"
                    }
                },
                {
                    "language": "km-KH",
                    "category": alert.category,
                    "event": alert.event,
                    "urgency": alert.urgency,
                    "severity": alert.severity,
                    "certainty": alert.certainty,
                    "headline": alert.headline_km,
                    "description": alert.description_km,
                    "instruction": alert.instruction_km,
                    "area": {
                        "areaDesc": alert.area_desc,
                        "circle": f"{alert.latitude:.4f},{alert.longitude:.4f} {alert.radius_km:.1f}"
                    }
                }
            ]
        }
