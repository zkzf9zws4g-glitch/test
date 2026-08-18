"""Parse a Form 4 ownershipDocument XML into transactions + owner info."""
import xml.etree.ElementTree as ET


def _text(el, path, default=None):
    if el is None:
        return default
    node = el.find(path)
    if node is None or node.text is None:
        return default
    return node.text.strip()


def _parse_owner(owner_el):
    rel = owner_el.find("reportingOwnerRelationship")
    return {
        "owner_cik_padded": _text(owner_el, "reportingOwnerId/rptOwnerCik"),
        "owner_name": _text(owner_el, "reportingOwnerId/rptOwnerName"),
        "is_director": _text(rel, "isDirector", "0") == "1",
        "is_officer": _text(rel, "isOfficer", "0") == "1",
        "is_ten_pct_owner": _text(rel, "isTenPercentOwner", "0") == "1",
        "is_other": _text(rel, "isOther", "0") == "1",
        "officer_title": _text(rel, "officerTitle", "") or "",
    }


def parse_form4_xml(xml_bytes):
    """Returns a dict with issuer info, primary owner info (a Form 4 can
    legally carry more than one <reportingOwner> block for joint filers;
    we pick the first owner flagged director/officer, else the first
    listed -- documented in filter_log.txt), the document-level Rule
    10b5-1 checkbox (schema field added Feb 2023), footnote text (used
    as a fallback 10b5-1 signal for pre-2023 filings), and the list of
    non-derivative transactions."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        return {"error": f"xml_parse_error: {e}"}

    doc_type = _text(root, "documentType")

    issuer = root.find("issuer")
    issuer_cik_padded = _text(issuer, "issuerCik")
    issuer_name = _text(issuer, "issuerName")
    issuer_ticker = _text(issuer, "issuerTradingSymbol")

    owners = [_parse_owner(o) for o in root.findall("reportingOwner")]
    if not owners:
        return {"error": "no_reporting_owner"}
    primary = next((o for o in owners if o["is_director"] or o["is_officer"]), owners[0])

    aff10b5 = _text(root, "aff10b5One")
    doc_level_10b5_1 = aff10b5 == "1"

    footnote_texts = []
    for fn in root.findall(".//footnotes/footnote"):
        if fn.text:
            footnote_texts.append(fn.text.strip())
    footnote_blob = " | ".join(footnote_texts)

    transactions = []
    for txn in root.findall(".//nonDerivativeTable/nonDerivativeTransaction"):
        code = _text(txn, "transactionCoding/transactionCode")
        txn_date = _text(txn, "transactionDate/value")
        shares = _text(txn, "transactionAmounts/transactionShares/value")
        price = _text(txn, "transactionAmounts/transactionPricePerShare/value")
        ad_code = _text(txn, "transactionAmounts/transactionAcquiredDisposedCode/value")
        transactions.append({
            "transaction_code": code,
            "transaction_date": txn_date,
            "shares": shares,
            "price_per_share": price,
            "acquired_disposed_code": ad_code,
        })

    return {
        "doc_type": doc_type,
        "issuer_cik_padded": issuer_cik_padded,
        "issuer_cik": issuer_cik_padded.lstrip("0") if issuer_cik_padded else None,
        "issuer_name": issuer_name,
        "issuer_ticker": issuer_ticker,
        "owner_cik_padded": primary["owner_cik_padded"],
        "owner_cik": primary["owner_cik_padded"].lstrip("0") if primary["owner_cik_padded"] else None,
        "owner_name": primary["owner_name"],
        "is_director": primary["is_director"],
        "is_officer": primary["is_officer"],
        "is_ten_pct_owner": primary["is_ten_pct_owner"],
        "is_other": primary["is_other"],
        "officer_title": primary["officer_title"],
        "all_owners": owners,
        "doc_level_10b5_1": doc_level_10b5_1,
        "footnote_blob": footnote_blob,
        "transactions": transactions,
    }
