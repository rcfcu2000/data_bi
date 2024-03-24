from nicegui import ui
import base_action
from crowd_top10 import crowd_top10

ui.button('Click me!', on_click=crowd_top10().run)

ui.run()
