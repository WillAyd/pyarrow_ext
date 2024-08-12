#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "nanoarrow/nanoarrow.h"

static PyObject *GetArrowArrayTuple(PyObject *obj) {
  if (!PyObject_HasAttrString(obj, "__arrow_c_array__")) {
    PyErr_SetString(PyExc_TypeError,
                    "Provided object has no method: '__arrow_c_array__'");
    return NULL;
  }

  PyObject *tup = PyObject_CallMethod(obj, "__arrow_c_array__", NULL);
  if (tup == NULL)
    return NULL;

  if (!PyTuple_Check(tup) || PyTuple_GET_SIZE(tup) != 2) {
    PyErr_SetString(PyExc_TypeError,
                    "__arrow_c_array__() did not return a two-tuple");
    return NULL;
  }

  return tup;
}

static ArrowErrorCode
UnpackArrowCapsulesToViews(PyObject *tup, struct ArrowSchemaView *schema_view,
                           struct ArrowArrayView *array_view,
                           struct ArrowError *error) {
  PyObject *schema_capsule = PyTuple_GET_ITEM(tup, 0);

  const struct ArrowSchema *schema =
      (const struct ArrowSchema *)PyCapsule_GetPointer(schema_capsule,
                                                       "arrow_schema");
  if (schema == NULL) {
    ArrowErrorSet(error, "Could not extract 'arrow_schema' capsule");
    return EINVAL;
  }

  PyObject *array_capsule = PyTuple_GET_ITEM(tup, 1);
  const struct ArrowArray *array =
      (const struct ArrowArray *)PyCapsule_GetPointer(array_capsule,
                                                      "arrow_array");
  if (array == NULL) {
    ArrowErrorSet(error, "Could not extract 'arrow_array' capsule");
    return EINVAL;
  }

  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(schema_view, schema, error));
  NANOARROW_RETURN_NOT_OK(
      ArrowArrayViewInitFromSchema(array_view, schema, error));
  ArrowErrorCode error_code;
  if ((error_code = ArrowArrayViewSetArray(array_view, array, error))) {
    ArrowArrayViewReset(array_view);
    return error_code;
  }

  return NANOARROW_OK;
}

static PyObject *ComputeSum(struct ArrowSchemaView *schema_view,
                            struct ArrowArrayView *array_view) {

  switch (schema_view->type) {
  case NANOARROW_TYPE_INT8:
  case NANOARROW_TYPE_INT16:
  case NANOARROW_TYPE_INT32:
  case NANOARROW_TYPE_INT64: {
    long long value = 0;
    for (int64_t i = 0; i < array_view->length; ++i) {
      if (ArrowArrayViewIsNull(array_view, i))
        continue;

      value += ArrowArrayViewGetIntUnsafe(array_view, i);
    }

    return PyLong_FromLongLong(value);
  }
  default:
    PyErr_SetString(PyExc_ValueError,
                    "Can only sum signed integral error types");
    return NULL;
  }
}

static PyObject *SumArray(PyObject *Py_UNUSED(self), PyObject *args) {
  PyObject *obj;
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  PyObject *tup = GetArrowArrayTuple(obj);
  if (tup == NULL)
    return NULL;

  struct ArrowSchemaView schema_view;
  struct ArrowArrayView array_view;
  struct ArrowError error;

  ArrowErrorCode error_code =
      UnpackArrowCapsulesToViews(tup, &schema_view, &array_view, &error);
  Py_DECREF(tup);
  if (error_code) {
    PyErr_SetString(PyExc_TypeError, error.message);
    return NULL;
  }

  PyObject *result = ComputeSum(&schema_view, &array_view);
  ArrowArrayViewReset(&array_view);

  return result;
};

static PyMethodDef pyarrow_ext_methods[] = {{"sum", (PyCFunction)SumArray,
                                             METH_VARARGS,
                                             PyDoc_STR("sums an Arrow array")},
                                            {}};

static PyModuleDef pyarrow_ext_def = {.m_base = PyModuleDef_HEAD_INIT,
                                      .m_name = "pyarrow_ext",
                                      .m_methods = pyarrow_ext_methods};

PyMODINIT_FUNC PyInit_pyarrow_ext(void) {
  return PyModuleDef_Init(&pyarrow_ext_def);
}
