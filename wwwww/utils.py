import csv
import hashlib
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Callable, Iterable

from loguru import logger
from selectolax.lexbor import LexborHTMLParser
from tqdm import tqdm


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_html(path: Path | str) -> LexborHTMLParser | None:
    """HTML ファイルをパースして :class:`LexborHTMLParser` を返す。

    ``read_bytes`` で読み込み、selectolax にバイト列を直接渡す（Python 側の
    UTF-8 デコードを 1 回省略してパース時間を短縮）。同パッケージの ``save_html``
    は UTF-8 固定で書き出すので、その形式の HTML に対しては安全。
    """
    try:
        return LexborHTMLParser(Path(path).read_bytes())
    except Exception as e:
        logger.error(f"[parse_html] {path} {type(e).__name__}: {e}")
        return None

def from_here(file: str) -> Callable[[str], Path]:
    base = Path(file).resolve().parent
    return lambda path: base / path

def append_csv(path: Path | str, row: dict) -> None:
    """``row`` を 1 行だけ CSV に追記する（ファイルが無ければ作成）。

    Excel 互換のため、**ファイル新規作成時のみ先頭に UTF-8 BOM** を書く
    （``utf-8-sig`` で open）。既存ファイルへの追記では BOM を書かない
    （中途 BOM は不正になるため）。ファイルが新規 / 空ならヘッダ行を書く。
    列順は ``row.keys()`` の順で、2 回目以降のキーずれは検知しない
    （pandas 版と同じ挙動）。
    """
    p = Path(path)
    try:
        _ensure_parent(p)
        need_header = not p.exists() or p.stat().st_size == 0
        encoding = 'utf-8-sig' if need_header else 'utf-8'
        with open(p, mode='a', newline='', encoding=encoding) as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if need_header:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        logger.error(f"[append_csv] {path} {row} {type(e).__name__}: {e}")

def write_parquet(path: Path | str, rows: list[dict]) -> None:
    """``rows`` を Parquet ファイルとして書き出す。

    pyarrow を直接使う（pandas 非依存）。``rows`` が空ならスキップ（警告のみ）。
    列スキーマは各列の最初の non-None 値から推論されるので、**同一キーで型が
    混在するとエラーになる**ことがある点に注意。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = Path(path)
    try:
        if not rows:
            logger.warning(f"[write_parquet] {path} no rows, skipped")
            return
        _ensure_parent(p)
        pq.write_table(pa.Table.from_pylist(rows), p)
    except Exception as e:
        logger.error(f"[write_parquet] {path} {type(e).__name__}: {e}")

def hash_name(key: str) -> str:
    return hashlib.md5(key.encode()).hexdigest()

def save_html(filepath: Path, html: str) -> bool:
    try:
        _ensure_parent(filepath)
        filepath.write_text(html, encoding="utf-8", errors="replace")
        return True
    except Exception as e:
        logger.error(f"[save_html] {filepath} {type(e).__name__}: {e}")
        return False

def add_log_file(path: Path | str, level: str = "WARNING") -> None:
    """loguru に、指定パスへ書き出すファイルシンクを 1 つ追加する。

    wwwww は内部で loguru を使ってログを出しており、この関数は
    ``logger.add(path, level=level, encoding='utf-8')`` を呼ぶだけの糖衣。
    親ディレクトリが無ければ作成する。既定の stderr シンクはそのまま残るため、
    追加呼び出しで "同時書き出し (tee)" になる。

    凝った構成（rotation / retention / 複数シンクなど）が必要な場合は、本関数を
    使わず ``from loguru import logger`` して ``logger.add(...)`` /
    ``logger.remove(...)`` を直接使うこと。

    同じ path で複数回呼ぶと、同じ行が重複して書かれるので注意。
    """
    p = Path(path)
    _ensure_parent(p)
    logger.add(p, level=level, encoding="utf-8")


class _SafeWorker:
    def __init__(self, fn: Callable) -> None:
        self.fn = fn

    def __call__(self, x):
        try:
            return self.fn(x)
        except Exception as e:
            logger.error(f"[pool_map] {type(e).__name__}: {e}")
            return None


def _auto_chunksize(n: int, workers: int | None) -> int:
    """``chunksize`` を自動で決める（``pool_map`` で未指定のとき）。

    子プロセスへは 1 件ずつより、まとめて送った方が速くなりやすい。そのまとめ数。

    ``w`` は並列数。引数で決まっていなければ ``os.cpu_count()``、それも無ければ 4。
    この **4** は「CPU が分からないときの仮の並列数」。式 ``n // (w * 4)`` の **4** とは別物。

    ``n // (w * 4)`` の方の **4** は経験則の係数。ざっくり言うとチャンクの個数が
    ``w * 4`` 前後になりやすく、負荷が均等ならワーカーあたりだいたい **4 回分の塊**
    を処理するイメージ（厳密ではない）。

    例: ``n=200``, ``w=5`` なら ``200 // 20 = 10`` が chunksize。全体は 20 チャンク、
    5 人で割ると 1 人あたり平均 4 チャンク（各 10 件）。

    結果は ``min(64, …)`` で上限。塊が大きすぎると **負荷が偏りやすい**。
    タスクの重さがバラバラなとき、太い塊の中に遅いのが多く入ったワーカーだけが
    長引き、他は先に終わって手待ちしがち（終盤のムラ）。塊を細かくすると配り直しの
    機会が増えて和らぎやすい。進捗バーも細かく動きやすい。

    ``max(1, …)`` で下限。割り算で 0 になっても最低 1 件は送る。
    """
    w = workers or os.cpu_count() or 4
    return max(1, min(64, n // (w * 4)))


def pool_map[T, R](
    worker: Callable[[T], R],
    items: Iterable[T],
    workers: int | None = None,
    *,
    chunksize: int | None = None,
) -> list[R | None]:
    """``ProcessPoolExecutor`` で ``worker`` を並列実行する。

    子プロセスで例外が出た分は ``None`` で返す。全体は止めない。
    進捗バーは常に tqdm。

    ``chunksize`` は子へまとめて送る件数。省略なら自動。
    進捗を細かくしたい・タスクの重さがバラバラで末尾に重いのが残る、なら ``chunksize=1``。
    """
    safe = _SafeWorker(worker)
    item_list = list(items)
    cs = chunksize if chunksize is not None else _auto_chunksize(len(item_list), workers)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        return list(
            tqdm(ex.map(safe, item_list, chunksize=cs), total=len(item_list), unit="file")
        )

def glob_paths(dir_path: str | Path, pattern: str = "*.html") -> list[str]:
    """
    ``dir_path`` 直下で ``pattern`` に一致するパスを ``str`` のリストで返す。

    ``str`` にしているのは ``pool_map`` 等のプロセスプールへ渡すとき pickle しやすくするため。
    """
    return [str(p) for p in Path(dir_path).glob(pattern)]
