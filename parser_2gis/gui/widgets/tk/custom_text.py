import tkinter as tk
from typing import Any


class CustomText(tk.Text):
    """Custom text widget that report on internal widget commands."""
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # Create a proxy for the underlying widget
        self._orig = self._w + '_orig'
        self.tk.call('rename', self._w, self._orig)
        self.tk.createcommand(self._w, self._proxy)

    def _proxy(self, *args) -> Any:
        # Let the actual widget perform the requested action
        cmd = (self._orig,) + args

        try:
            result = self.tk.call(cmd)
        except tk.TclError:
            result = ''

        # Generate an event if something was added or deleted,
        # or the cursor position changed.
        if (
            args[0] in ('insert', 'replace', 'delete')
            or args[0:3] == ('mark', 'set', 'insert')
            or args[0:2] == ('xview', 'moveto')
            or args[0:2] == ('xview', 'scroll')
            or args[0:2] == ('yview', 'moveto')
            or args[0:2] == ('yview', 'scroll')
        ):

            self.event_generate('<<Change>>', when='tail')

        # Return what the actual widget returned
        return result
