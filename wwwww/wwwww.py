from __future__ import annotations

import html
import random
import re
import time
import unicodedata as ud
from datetime import datetime, timezone
from urllib.parse import urljoin

from loguru import logger
from patchright.sync_api import Page as PatchrightPage, ElementHandle as PatchrightElementHandle
from playwright.sync_api import Page as PlaywrightPage, ElementHandle as PlaywrightElementHandle
from selectolax.lexbor import LexborHTMLParser, LexborNode


Page = PatchrightPage | PlaywrightPage
ElementHandle = PatchrightElementHandle | PlaywrightElementHandle

_SAVED_HTML_META_URL = "wwwww:url"
_SAVED_HTML_META_SAVED_AT = "wwwww:saved_at"


def wrap_page(page: Page) -> WrappedPage:
    return WrappedPage(page)

class _PageScoped:
    _page: Page

    def wrap_element(self, elem: ElementHandle | None) -> WrappedElement:
        return WrappedElement(self._page, elem)

    def wrap_element_group(self, elems: list[WrappedElement]) -> WrappedElementGroup:
        return WrappedElementGroup(self._page, elems)


def wrap_parser(parser: LexborHTMLParser) -> WrappedParser:
    return WrappedParser(parser)

def wrap_node(node: LexborNode | None) -> WrappedNode:
    return WrappedNode(node)

def wrap_node_group(nodes: list[WrappedNode]) -> WrappedNodeGroup:
    return WrappedNodeGroup(nodes)


class WrappedPage(_PageScoped):
    def __init__(self, page: Page) -> None:
        self._page = page

    @property
    def raw(self) -> Page:
        return self._page

    def s(self, selector: str) -> WrappedElement:
        elem = self._page.query_selector(selector)
        return self.wrap_element(elem)

    def ss(self, selector: str) -> WrappedElementGroup:
        elems = self._page.query_selector_all(selector)
        return self.wrap_element_group([self.wrap_element(e) for e in elems])

    def goto(
        self,
        url: str | None,
        try_cnt: int = 3,
        wait_range: tuple[float, float] = (3, 5),
        sleep_after: tuple[float, float] | None = (1, 2),
    ) -> bool:
        if not url:
            return False
        for i in range(try_cnt):
            try:
                if self._page.goto(url) is not None:
                    if sleep_after is not None:
                        time.sleep(random.uniform(*sleep_after))
                    return True
                else:
                    reason = "response is None"
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
            logger.warning(f"[goto] {url} ({i+1}/{try_cnt}) {reason}")
            if i + 1 < try_cnt:
                time.sleep(random.uniform(*wait_range))
        logger.error(f"[goto] giving up: {url}")
        return False

    def wait(self, selector: str, state: str = "attached", timeout: int = 15000) -> WrappedElement:
        try:
            elem = self._page.wait_for_selector(selector, state=state, timeout=timeout)
            return self.wrap_element(elem)
        except Exception as e:
            logger.warning(f"[wait] {type(e).__name__}: {e} | selector={selector!r} | url={self._page.url}")
            return self.wrap_element(None)

    def html(self, with_url: bool = False, with_saved_at: bool = False) -> str:
        content = self._page.content()
        metas: list[str] = []
        if with_url:
            metas.append(
                f'<meta name="{_SAVED_HTML_META_URL}" content="{html.escape(self._page.url)}">'
            )
        if with_saved_at:
            ts = datetime.now(timezone.utc).isoformat()
            metas.append(f'<meta name="{_SAVED_HTML_META_SAVED_AT}" content="{ts}">')
        return ''.join(metas) + content


class WrappedElement(_PageScoped):
    def __init__(self, page: Page, elem: ElementHandle | None) -> None:
        self._page = page
        self._elem = elem

    @property
    def raw(self) -> ElementHandle | None:
        return self._elem

    def s(self, selector: str) -> WrappedElement:
        elem = self._elem.query_selector(selector) if self._elem else None
        return self.wrap_element(elem)

    def ss(self, selector: str) -> WrappedElementGroup:
        elems = self._elem.query_selector_all(selector) if self._elem else []
        return self.wrap_element_group([self.wrap_element(e) for e in elems])

    def next(self, selector: str) -> WrappedElement:
        if self._elem is None:
            return self.wrap_element(None)
        try:
            elem = self._elem.evaluate_handle(
                """(el, sel) => {
                    let cur = el.nextElementSibling;
                    while (cur) {
                        if (cur.matches(sel)) return cur;
                        cur = cur.nextElementSibling;
                    }
                    return null;
                }""",
                selector,
            ).as_element()
            return self.wrap_element(elem)
        except Exception as e:
            logger.error(f"[next] {self._elem} {type(e).__name__}: {e}")
            return self.wrap_element(None)

    @property
    def text(self) -> str | None:
        if self._elem is None:
            return None
        return text if (text := self._elem.text_content()) else None

    def attr(self, attr_name: str) -> str | None:
        if self._elem is None:
            return None
        return attr if (attr := self._elem.get_attribute(attr_name)) else None

    @property
    def url(self) -> str | None:
        if self._elem is None:
            return None
        if not (href := self._elem.get_attribute('href')):
            return None
        if not (h := href.strip()):
            return None
        if re.search(r'(?i)^(?:#|javascript:|mailto:|tel:|data:)', h):
            return None
        return urljoin(self._page.url, h)


class WrappedElementGroup(_PageScoped):
    def __init__(self, page: Page, elems: list[WrappedElement]) -> None:
        self._page = page
        self._elems = elems

    @property
    def raw(self) -> list[WrappedElement]:
        return self._elems

    @property
    def re(self) -> ElementGrep:
        pairs: list[tuple[str, WrappedElement]] = []
        for e in self._elems:
            if (t := e.text):
                pairs.append((ud.normalize('NFKC', t), e))
        return ElementGrep(self._page, pairs)

    @property
    def urls(self) -> list[str | None]:
        return [e.url for e in self._elems]


class ElementGrep(_PageScoped):
    def __init__(self, page: Page, pairs: list[tuple[str, WrappedElement]]) -> None:
        self._page = page
        self._pairs = pairs

    def s(self, pattern: str) -> WrappedElement:
        try:
            prog = re.compile(pattern)
            for text, e in self._pairs:
                if prog.search(text):
                    return e
        except Exception as e:
            logger.warning(f"[grep] {type(e).__name__}: {e} | pattern={pattern!r}")
        return self.wrap_element(None)

    def ss(self, pattern: str) -> WrappedElementGroup:
        try:
            prog = re.compile(pattern)
            filtered = [e for text, e in self._pairs if prog.search(text)]
        except Exception as e:
            logger.warning(f"[grep] {type(e).__name__}: {e} | pattern={pattern!r}")
            filtered = []
        return self.wrap_element_group(filtered)


class WrappedParser:
    def __init__(self, parser: LexborHTMLParser) -> None:
        self._parser = parser

    @property
    def raw(self) -> LexborHTMLParser:
        return self._parser

    def s(self, selector: str) -> WrappedNode:
        node = self._parser.css_first(selector)
        return wrap_node(node)

    def ss(self, selector: str) -> WrappedNodeGroup:
        nodes = self._parser.css(selector)
        return wrap_node_group([wrap_node(n) for n in nodes])

    @property
    def url(self) -> str | None:
        node = self._parser.css_first(f'meta[name="{_SAVED_HTML_META_URL}"]')
        if node is None:
            return None
        return node.attributes.get('content') or None

    @property
    def saved_at(self) -> str | None:
        node = self._parser.css_first(f'meta[name="{_SAVED_HTML_META_SAVED_AT}"]')
        if node is None:
            return None
        return node.attributes.get('content') or None


class WrappedNode:
    def __init__(self, node: LexborNode | None) -> None:
        self._node = node

    @property
    def raw(self) -> LexborNode | None:
        return self._node
    
    def s(self, selector: str) -> WrappedNode:
        node = self._node.css_first(selector) if self._node else None
        return wrap_node(node)

    def ss(self, selector: str) -> WrappedNodeGroup:
        nodes = self._node.css(selector) if self._node else []
        return wrap_node_group([wrap_node(n) for n in nodes])

    def next(self, selector: str) -> WrappedNode:
        if self._node is None:
            return wrap_node(None)
        cur = self._node.next
        while cur is not None:
            if cur.is_element_node and cur.css_matches(selector):
                return wrap_node(cur)
            cur = cur.next
        return wrap_node(None)

    @property
    def text(self) -> str | None:
        if self._node is None:
            return None
        return text if (text := self._node.text()) else None

    def attr(self, attr_name: str) -> str | None:
        if self._node is None:
            return None
        return attr if (attr := self._node.attributes.get(attr_name)) else None


class WrappedNodeGroup:
    def __init__(self, nodes: list[WrappedNode]) -> None:
        self._nodes = nodes
    
    @property
    def raw(self) -> list[WrappedNode]:
        return self._nodes

    @property
    def re(self) -> NodeGrep:
        pairs: list[tuple[str, WrappedNode]] = []
        for n in self._nodes:
            if (t := n.text):
                pairs.append((ud.normalize('NFKC', t), n))
        return NodeGrep(pairs)


class NodeGrep:
    def __init__(self, pairs: list[tuple[str, WrappedNode]]) -> None:
        self._pairs = pairs

    def s(self, pattern: str) -> WrappedNode:
        try:
            prog = re.compile(pattern)
            for text, n in self._pairs:
                if prog.search(text):
                    return n
        except Exception as e:
            logger.warning(f"[grep] {type(e).__name__}: {e} | pattern={pattern!r}")
        return wrap_node(None)

    def ss(self, pattern: str) -> WrappedNodeGroup:
        try:
            prog = re.compile(pattern)
            filtered = [n for text, n in self._pairs if prog.search(text)]
        except Exception as e:
            logger.warning(f"[grep] {type(e).__name__}: {e} | pattern={pattern!r}")
            filtered = []
        return wrap_node_group(filtered)
