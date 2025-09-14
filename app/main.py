from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.textinput import TextInput
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.gridlayout import GridLayout

class HomePage(BoxLayout):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.orientation = 'vertical'
        self.padding = 10
        self.spacing = 10

        # Title
        self.add_widget(Label(text="📊 Stock Market Dashboard",
                              font_size=24,
                              size_hint=(1, 0.1)))

        # Search Bar
        self.search = TextInput(
            hint_text="Search company (e.g., Reliance)",
            size_hint=(1, 0.1),
            multiline=False
        )
        self.add_widget(self.search)

        # Buttons Row
        button_row = BoxLayout(size_hint=(1, 0.1), spacing=10)
        button_row.add_widget(Button(text="Predictions"))
        button_row.add_widget(Button(text="Live Stocks"))
        self.add_widget(button_row)

        # Mini Charts Grid (placeholder)
        grid = GridLayout(cols=3, spacing=10, size_hint=(1, 0.7))
        for i in range(6):  # just 6 placeholders now
            grid.add_widget(Button(text=f"Chart {i+1}"))
        self.add_widget(grid)


class StockApp(App):
    def build(self):
        return HomePage()

if __name__ == '__main__':
    StockApp().run()
