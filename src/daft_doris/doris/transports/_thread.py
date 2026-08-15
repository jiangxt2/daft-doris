# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cancellation-safe ownership for one task-dedicated synchronous I/O thread."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar

_T = TypeVar("_T")
logger = logging.getLogger(__name__)


class TaskThread:
    """Run operations serially and guarantee a submitted close is never cancelled."""

    def __init__(self, *, name: str) -> None:
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=name)
        self._close_future: asyncio.Future[None] | None = None

    async def call(self, operation: Callable[[], _T]) -> _T:
        """Run one synchronous operation on the owned thread."""
        return await asyncio.get_running_loop().run_in_executor(self._executor, operation)

    async def close(self, operation: Callable[[], None]) -> None:
        """Submit close once and let it finish even if the awaiting task is cancelled again."""
        if self._close_future is None:
            self._close_future = asyncio.get_running_loop().run_in_executor(
                self._executor, operation
            )
            self._close_future.add_done_callback(self._shutdown)
        await asyncio.shield(self._close_future)

    def _shutdown(self, _future: asyncio.Future[None]) -> None:
        if not _future.cancelled():
            failure = _future.exception()
            if failure is not None:
                logger.warning(
                    "Doris task-thread resource close failed (%s)",
                    type(failure).__name__,
                )
        self._executor.shutdown(wait=False, cancel_futures=False)
