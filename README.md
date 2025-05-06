# yapper-python
Message broker for the digital campus

## Installation

**Note:** `yapper-python` is not yet published, but once it is, this is the expected usage.

To install `yapper-python`, use pip:

```bash
pip install yapper-python
```

## Usage

Yapper has a basic interface very similar to Flask.

To use `yapper-python` as an event client listening for events:

```python
from yapper import Yapper, Event

yap = Yapper('campus.myapp')

@yap.on_event('google.forms.submit')
def on_google_forms_submit(event: Event) -> None:
    """Handle event when a google form is submitted"""
    # Event has a `data` attribute containing event data
    user = event.data['email']
    cca = event.data['cca']
    # Assuming successful form submission adds user to the CCA
    yap.emit(
        'campus.circles.user.add',
        data={'user': user, 'circle': cca}
    )

# Begin listening for events and handling them
yap.run()
```

Dynamic event handler registration is not yet designed, but is on the roadmap.

## Features

- Event-driven architecture for the digital campus.
- Easy-to-use API for emitting and handling events.
- Supports custom event handlers.
