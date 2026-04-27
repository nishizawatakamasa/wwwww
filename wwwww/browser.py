from collections.abc import Iterator
from contextlib import contextmanager

from camoufox.sync_api import Camoufox
from patchright.sync_api import Page as PatchrightPage, sync_playwright
from playwright.sync_api import Page as PlaywrightPage

Page = PatchrightPage | PlaywrightPage


@contextmanager
def patchright_page() -> Iterator[Page]:
    with sync_playwright() as pw:
        with pw.chromium.launch(
            channel='chrome',
            headless=False,
        ) as browser:
            with browser.new_context() as context:
                page = context.new_page()
                yield page


@contextmanager
def camoufox_page() -> Iterator[Page]:
    with Camoufox(
        headless=False,
        humanize=True,
    ) as browser:
        page = browser.new_page()
        yield page
