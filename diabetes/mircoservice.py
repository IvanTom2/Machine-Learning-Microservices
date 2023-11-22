from functools import partial
from typing import Callable
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from database import (
    DataBaseInterface,
    create_db_interface,
    destroy_db_interface,
    DB_KEY,
)
from pipeline import create_pipeline, PIPELINE_KEY
from notation import PageTemplate, Patient


routes = web.RouteTableDef()


def read_template(path: str) -> str:
    with open(path, "r") as file:
        content = file.read()
    return content


async def check_patient_id(app, request: Request):
    # Проверяем наличие куки с именем 'patient_id'
    patient_id = request.cookies.get(Patient.PATIENT_ID.name)
    if not patient_id:
        return web.HTTPFound("/patient_id")
    return await request.handler(app, request)


@routes.get("/patient_id")
async def products(request: Request) -> Response:
    return web.Response(text="Это страница ввода Patient ID")


@routes.get("/appointment")
async def appointment(request: Request) -> Response:
    html = read_template(PageTemplate.APPOINTMENT)
    return web.Response(text=html, content_type="text/html")


@routes.get("/menu")
async def menu(request: Request) -> Response:
    return web.Response(text="Это страница Меню")


app = web.Application()
app.on_startup.append(partial(create_db_interface, connections_count=6))
app.on_startup.append(partial(create_pipeline))

app.middlewares.append(check_patient_id)

app.on_cleanup.append(partial(destroy_db_interface))

app.add_routes(routes)
web.run_app(app, host="127.0.0.1", port=8000)
