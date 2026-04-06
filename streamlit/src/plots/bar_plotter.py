import plotly.express as px


class BarPlotter:
    @staticmethod
    def plot_bar(df, x, y, title):
        fig = px.bar(df, y=y, x=x, title=title)

        return fig
