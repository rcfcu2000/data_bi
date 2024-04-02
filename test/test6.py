from nicegui import ui
<<<<<<< HEAD
import every_one_task.base_action as base_action
from every_one_task.crowd_top10 import crowd_top10
=======
import base_action
from crowd_top10 import crowd_top10
>>>>>>> cd5a759c91eaf1f85c123e8bf256658f36943fcb

ui.button('Click me!', on_click=crowd_top10().run)

ui.run()
