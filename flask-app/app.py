import base64
import io

import matplotlib.pyplot as plt
import pandas as pd
from flask import Flask, Response, render_template

app = Flask(__name__)


def plot_colored_dots(csv_filepath: str) -> str:
    try:
        df = pd.read_csv(csv_filepath)
        x = df.iloc[:, 0]
        y = df.iloc[:, 1]
        colors = df.iloc[:, 2]

        plt.figure(figsize=(8, 6))
        plt.scatter(x, y, c=colors, cmap="viridis")
        plt.xlabel("value")
        plt.ylabel("time delta")
        plt.title("Wash trading cluster detection")
        plt.colorbar(label="cluster")

        img_buf = io.BytesIO()
        plt.savefig(img_buf, format="png")
        img_buf.seek(0)
        plt.close()

        img_base64 = base64.b64encode(img_buf.read()).decode("utf8")
        return img_base64

    except FileNotFoundError:
        return "Error: CSV file not found."
    except (IndexError, ValueError) as e:
        return f"Error processing CSV data: {e}"
    except Exception as e:
        return f"An unexpected error occurred: {e}"


@app.route("/")
def plot_endpoint() -> Response:
    csv_filepath = "../airflow/data/wash_trading_plot_data.csv"
    img_base64 = plot_colored_dots(csv_filepath)

    if isinstance(img_base64, str) and img_base64.startswith("Error"):
        return render_template("error.html", error_message=img_base64)
    return render_template("plot.html", plot_img=img_base64)


if __name__ == "__main__":
    app.run(debug=True)
