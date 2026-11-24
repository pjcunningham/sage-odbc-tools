from contextlib import contextmanager


@contextmanager
def progress_items(items, progress=None, label=''):
    if progress is None:
        yield items
        return

    with progress(items, length=len(items), label=label) as progress_iterator:
        yield progress_iterator
