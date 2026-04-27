# wwwww
Web Wrapper Without Weight  
自分用・非汎用

## インストール
`uv add wwwww`  
`uv run patchright install chromium`  
`uv run camoufox fetch`  


## 使用例
### スクレイピング
```python
from wwwww import wrap_page
from wwwww.browser import patchright_page
from wwwww.utils import add_log_file, append_csv, from_here

fh = from_here(__file__)
add_log_file(fh('log/scraping.log'))

with patchright_page() as page:
    p = wrap_page(page)

    p.goto('https://www.foobarbaz1.jp')
    pref_urls = p.ss('li.item > ul > li > a').urls

    classroom_urls = []
    for i, url in enumerate(pref_urls, 1):
        print(f'pref_urls {i}/{len(pref_urls)}')
        if not p.goto(url):
            append_csv(fh('csv/failed.csv'), {'url': url, 'reason': 'goto'})
            continue
        classroom_urls.extend(p.ss('.school-area h4 a').urls)

    for i, url in enumerate(classroom_urls, 1):
        print(f'classroom_urls {i}/{len(classroom_urls)}')
        if not p.goto(url):
            append_csv(fh('csv/failed.csv'), {'url': url, 'reason': 'goto'})
            continue
        th_grep = p.ss('th').re
        append_csv(fh('csv/scrape.csv'), {
            'URL': page.url,
            '教室名': p.s('h1 .text01').text,
            '住所': p.s('.item .mapText').text,
            '電話番号': p.s('.item .phoneNumber').text,
            'HP': th_grep.s(r'ホームページ').next('td').s('a').url,
            '営業時間': th_grep.s(r'営業時間').next('td').text,
            '定休日': th_grep.s(r'定休日').next('td').text,
        })
```

### スクレイピング(HTML丸ごと保存)

```python
from wwwww import wrap_page
from wwwww.browser import camoufox_page
from wwwww.utils import add_log_file, append_csv, from_here, hash_name, save_html

fh = from_here(__file__)
add_log_file(fh('log/scraping.log'))

with camoufox_page() as page:
    p = wrap_page(page)

    p.goto('https://www.foobarbaz1.jp')
    item_urls = p.ss('ul.items > li > a').urls

    for i, url in enumerate(item_urls, 1):
        print(f'item_urls {i}/{len(item_urls)}')
        if not p.goto(url):
            append_csv(fh('csv/failed.csv'), {'url': url, 'reason': 'goto'})
            continue
        file_name = f'{hash_name(url)}.html'
        if not save_html(fh('html') / file_name, p.html(with_url=True)):
            append_csv(fh('csv/failed.csv'), {'url': url, 'reason': 'save_html'})
            continue
```

### ローカルHTMLからデータ抽出&Parquet出力

```python
from wwwww import wrap_parser
from wwwww.utils import add_log_file, from_here, parse_html, write_parquet

fh = from_here(__file__)
add_log_file(fh('log/scraping.log'))

results = []
for i, file_path in enumerate(fh('html').glob('*.html')):
    print(f'html {i}')
    if not (parser := parse_html(file_path)):
        continue
    p = wrap_parser(parser)
    dt_grep = p.ss('dt').re
    results.append({
        'URL': p.url,
        'file_name': file_path.name,
        '教室名': p.s('h1 .text02').text,
        '住所': p.s('.item .mapText').text,
        '所在地': dt_grep.s(r'所在地').next('dd').text,
        '交通': dt_grep.s(r'交通').next('dd').text,
        '物件番号': dt_grep.s(r'物件番号').next('dd').text,
    })
write_parquet(fh('parquet/extract.parquet'), results)
```

### ローカルHTMLからデータ抽出&Parquet出力(並列処理)
```python
from pathlib import Path

from wwwww import wrap_parser
from wwwww.utils import from_here, glob_paths, parse_html, pool_map, write_parquet

def main():
    fh = from_here(__file__)
    html_paths = glob_paths(fh('html'), '*.html')
    results = [r for r in pool_map(extract, html_paths) if r]
    write_parquet(fh('parquet/extract.parquet'), results)

def extract(file_path: str) -> dict | None:
    if not (parser := parse_html(file_path)):
        return None
    p = wrap_parser(parser)
    dt_grep = p.ss('dt').re
    return {
        'URL': p.url,
        'file_name': Path(file_path).name,
        '教室名': p.s('h1 .text02').text,
        '住所': p.s('.item .mapText').text,
        '所在地': dt_grep.s(r'所在地').next('dd').text,
        '交通': dt_grep.s(r'交通').next('dd').text,
        '価格': dt_grep.s(r'価格').next('dd').text,
        '設備・条件': dt_grep.s(r'設備').next('dd').text,
        '備考': dt_grep.s(r'備考').next('dd').text,
    }

if __name__ == '__main__':
    main()
```

## License - ライセンス

[MIT](./LICENSE)
