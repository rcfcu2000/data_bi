from nicegui import ui
import every_one_task.base_action as base_action
from every_one_task.crowd_top10 import crowd_top10

ui.button('Click me!', on_click=crowd_top10().run)

ui.run()
