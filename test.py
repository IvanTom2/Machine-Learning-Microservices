import plotly
import pandas as pd

data = pd.DataFrame()


def target_count():
    trace = plotly.graph_objs.Bar(
        x=data["Outcome"].value_counts().values.tolist(),
        y=["healthy", "diabetic"],
        orientation="h",
        text=data["Outcome"].value_counts().values.tolist(),
        textfont=dict(size=15),
        textposition="auto",
        opacity=0.8,
        marker=dict(
            color=["lightskyblue", "gold"], line=dict(color="#0000000", width=1.5)
        ),
    )

    layout = dict(title='Count of target variable "Outcome"')
    figure = dict(data=[trace], layout=layout)
    plotly.offline.iplot(figure)


target_count()
