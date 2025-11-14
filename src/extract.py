# -*- coding: utf-8 -*-
# pip install s3fs canedge_browser asammdf pandas python-dateutil pytz

import os, re, json, shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
import pytz
from asammdf import MDF
import s3fs, canedge_browser

# ====== CONFIG S3/MinIO ======
ENDPOINT   = "http://179.106.217.150:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "Nuphi@123"
BUCKET     = "canedge-logs"
DEVICE     = "3B7C3AEB"

# ====== PATHS ======
BASE_DIR    = Path("/home/chdt/sniffer")
DEC_DIR     = BASE_DIR / "convertido-decimal"   # <decimal>.(MF4|MFC)
ASAMMDF_DIR = BASE_DIR / "asammdf"              # saídas e checkpoints

DL_STATE   = ASAMMDF_DIR / ".dl_checkpoint.json"
PROC_STATE = ASAMMDF_DIR / ".proc_checkpoint.json"

# ====== FILTROS ======
TIMEZONE   = pytz.timezone("America/Manaus")
START_DATE = "2025-11-03"           # use None para desativar
STABILIZE_SECONDS = 180             # ignore objetos mais novos que isso

# DBCs
DBC_DIR    = Path("/home/chdt/sniffer/dbc_files")
DBC_LIST   = [
    "Fuell_Cell_msgs.dbc",
    "BAT_All_msgs_BMS_CTS.dbc",
    "BAT_Orion_CANBUS_Corrigido.dbc",
    "CSS-Electronics-SAE-J1939-DEMO.dbc",
    "database-01.09.dbc",
    "DCDC_CAN_Network_v1.23.dbc",
    "tp_uds_nissan.dbc",
]

HEX_RE = re.compile(r"([0-9A-Fa-f]{8,})")

def ensure_dirs():
    for d in (DEC_DIR, ASAMMDF_DIR):
        d.mkdir(parents=True, exist_ok=True)

def to_epoch_start() -> int | None:
    if not START_DATE:
        return None
    dt_local = TIMEZONE.localize(datetime.strptime(START_DATE, "%Y-%m-%d"))
    dt_utc = dt_local.astimezone(timezone.utc)
    return int(dt_utc.timestamp())

from datetime import datetime as _dt
EPOCH_START = to_epoch_start()
print(f"[CFG] START_DATE={START_DATE} -> EPOCH_START={EPOCH_START} TZ={TIMEZONE}")

def get_fs():
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT, "region_name": "us-east-1"},
        anon=False,
    )

def load_ck(path: Path) -> int:
    if path.exists():
        try:
            return int(json.loads(path.read_text()).get("last_decimal", -1))
        except Exception:
            return -1
    return -1

def save_ck(path: Path, dec: int):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"last_decimal": int(dec), "updated_at": _dt.now().isoformat()}
    path.write_text(json.dumps(data, indent=2))
    print(f"[CK] {path.name} = {dec}")

def extract_hex_from_name(name: str) -> str | None:
    core = name.rsplit(".", 1)[0]
    if "-" in core:
        after = core.rsplit("-", 1)[-1]
        if re.fullmatch(r"[0-9A-Fa-f]{8,}", after):
            return after
    m = HEX_RE.search(name)
    return m.group(1) if m else None

def _now_utc():
    return _dt.now(timezone.utc)

# LISTAGEM: arquivos planos + estabilização
def remote_list_canedge(fs) -> list[str]:
    base = f"{BUCKET}/{DEVICE}"
    paths = canedge_browser.get_log_files(fs, [base])
    out = []
    now = _now_utc()
    for p in paths:
        try:
            if fs.isdir(p):
                continue
            ext = os.path.splitext(p)[1].upper()
            if ext not in (".MFC", ".MF4"):
                continue
            info = fs.info(p)
            lm = info.get("LastModified")
            lm_dt = None
            if isinstance(lm, str):
                try:
                    lm_dt = _dt.fromisoformat(lm.replace("Z", "+00:00"))
                except Exception:
                    lm_dt = None
            elif isinstance(lm, _dt):
                lm_dt = lm
            if lm_dt:
                if (now - lm_dt.replace(tzinfo=timezone.utc)) < timedelta(seconds=STABILIZE_SECONDS):
                    continue
            out.append(p)
        except Exception:
            out.append(p)
    return out

# VERIFICAÇÃO RÁPIDA DE HEADER MDF
def _looks_like_mdf(local_path: Path) -> bool:
    try:
        with open(local_path, "rb") as f:
            f.seek(0x40)
            return f.read(4) == b"##HD"
    except Exception:
        return False

# DOWNLOAD INCREMENTAL
def download_incremental():
    fs = get_fs()
    logs = remote_list_canedge(fs)
    if not logs:
        print("[DL] nenhum objeto no bucket")
        return 0, -1

    last_dl = load_ck(DL_STATE)
    min_dec = last_dl if EPOCH_START is None else max(last_dl, EPOCH_START - 1)

    new_max = last_dl
    downloaded = 0

    for remote in sorted(logs):
        base = os.path.basename(remote)
        ext  = os.path.splitext(base)[1].upper()
        if ext not in (".MF4", ".MFC"):
            continue

        hex_part = extract_hex_from_name(base)
        if not hex_part:
            print(f"[SKIP-NAME] sem HEX: {base}")
            continue

        try:
            dec_ts = int(hex_part, 16)
        except ValueError:
            print(f"[SKIP-HEX] inválido: {base}")
            continue

        if dec_ts <= min_dec:
            continue

        dst_dec = DEC_DIR / f"{dec_ts}{ext}"
        if dst_dec.exists():
            print(f"[HIT] já existe: {dst_dec.name}")
            new_max = max(new_max, dec_ts)
            continue

        print(f"[DL] {remote} -> {dst_dec} (src_hex={hex_part} dec={dec_ts} ext={ext})")
        with fs.open(remote, "rb") as rf, open(dst_dec, "wb") as lf:
            shutil.copyfileobj(rf, lf)

        # Validação e fallback
        if not _looks_like_mdf(dst_dec):
            print(f"[WARN] header inesperado em {dst_dec.name}; tentando fallback de extensão...")
            alt_ext = ".MF4" if ext == ".MFC" else ".MFC"
            alt_remote = remote[:-4] + alt_ext
            try:
                with fs.open(alt_remote, "rb") as rf, open(dst_dec.with_suffix(alt_ext), "wb") as lf:
                    shutil.copyfileobj(rf, lf)
                if _looks_like_mdf(dst_dec.with_suffix(alt_ext)):
                    print(f"[FIX] baixado fallback {alt_remote} -> {dst_dec.with_suffix(alt_ext).name}")
                    try: dst_dec.unlink()
                    except Exception: pass
                    dst_dec = dst_dec.with_suffix(alt_ext)
                else:
                    print(f"[ERR] fallback também inválido: {alt_remote}")
            except Exception as e:
                print(f"[ERR] não conseguiu fallback {alt_remote}: {e}")

        downloaded += 1
        new_max = max(new_max, dec_ts)

    if downloaded > 0 and new_max > last_dl:
        save_ck(DL_STATE, new_max)
    else:
        print(f"[DL] nenhum novo (checkpoint={last_dl}, epoch_start={EPOCH_START})")

    return downloaded, new_max

def pick_new_decimals(last_proc: int) -> list[Path]:
    cand = []
    for p in list(DEC_DIR.glob("*.MF4")) + list(DEC_DIR.glob("*.MFC")):
        try:
            dec = int(Path(p).stem)
            if (EPOCH_START is None or dec >= EPOCH_START) and dec > last_proc and Path(p).is_file():
                cand.append((dec, Path(p)))
        except Exception:
            continue
    cand.sort(key=lambda x: x[0])
    return [p for _, p in cand]

# PROCESSAMENTO: ignora inválidos e concatena apenas válidos
def process_incremental():
    last_proc = load_ck(PROC_STATE)
    new_files = pick_new_decimals(last_proc)
    if not new_files:
        print(f"[PROC] sem novos decimais (checkpoint={last_proc}, epoch_start={EPOCH_START})")
        return 0, None, None, last_proc

    print("[PROC] arquivos na faixa:")
    for p in new_files:
        print(f"       - {p.name}")

    valid_files = []
    for p in new_files:
        if _looks_like_mdf(p):
            valid_files.append(p)
        else:
            print(f"[SKIP] header inválido (não MDF): {p.name}")

    if not valid_files:
        print("[PROC] nenhum arquivo válido para concatenar")
        return 0, None, None, last_proc

    first_dec = int(valid_files[0].stem)
    last_dec  = int(valid_files[-1].stem)
    print(f"[PROC] válidos={len(valid_files)} faixa={first_dec}..{last_dec}")

    entradas = [str(p) for p in valid_files]
    mdf_concat = MDF.concatenate(entradas, sync=False)

    out_concat  = ASAMMDF_DIR / f"concatenado-incr-{first_dec}-{last_dec}.mf4"
    mdf_concat.save(str(out_concat), overwrite=True)
    print(f"[WRITE] {out_concat}")

    print("[DECODE] via DBCs...")
    db_dict = {"CAN": [(str(DBC_DIR / n), 0) for n in DBC_LIST]}
    decoded = mdf_concat.extract_bus_logging(database_files=db_dict)

    out_decoded = ASAMMDF_DIR / f"concatenado-incr-{first_dec}-{last_dec}-final.mf4"
    decoded.save(str(out_decoded), overwrite=True)
    print(f"[WRITE] {out_decoded}")

    save_ck(PROC_STATE, last_dec)
    return len(valid_files), out_concat, out_decoded, last_dec

def main():
    ensure_dirs()
    print(f"[{_dt.now().isoformat()}] Pipeline incremental (arquivos planos .MF4/.MFC) iniciado")

    dl_count, dl_ck = download_incremental()
    print(f"[DL] novos baixados: {dl_count}")

    proc_count, out_concat, out_decoded, proc_ck = process_incremental()
    print(f"[PROC] novos processados: {proc_count}")

if __name__ == "__main__":
    main()

