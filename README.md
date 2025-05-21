# yapper-python

Message broker for the digital campus.

## Installation

**Note:** `campus-yapper` is not yet published on PyPI, hence installation via `pip` is not yet supported.

First, clone the repository:

```bash
git clone https://github.com/nyjc-computing/yapper-python
```

Then, install the package using `poetry`:

```bash
cd yapper-python
poetry install
```

## Usage

Yapper has a basic interface very similar to Flask.

To use `campus-yapper` as an event client listening for events:

```python
import campus_yapper

# Identify the app using a unique client ID string
yapper = campus_yapper.create('campus.myapp')

@yapper.on_event('google.forms.submit')
def on_google_forms_submit(event: Event) -> None:
    """Handle event when a google form is submitted"""
    # Assume event has a `data` attribute containing event data
    user = event.data['email']
    cca = event.data['cca']
    # Assuming successful form submission adds user to the CCA
    yap.emit(
        'campus.circles.user.add',
        data={'user': user, 'circle': cca}
    )

# Begin listening for events and handling them
yapper.run()
```

Dynamic event handler registration is not yet designed, but is on the roadmap.

## Features

- Event-driven architecture for the digital campus.
- Easy-to-use API for emitting and handling events.
- Supports custom event handlers.
