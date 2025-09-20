import azure.functions as func
import append_blob_function

app = func.FunctionApp()
append_blob_function.register(app)
