The code from this repository reflects the final state of the application built in my [Build Python Extensions for Apache Arrow Data with nanoarrow](https://youtu.be/EhUnmXPjTy8?si=s_903aWaRgb-bBAQ) on YouTube.

## How to build

If you don't already have meson, be sure to install that first:

```sh
python -m pip install meson
```

You can then configure and build the project:

```sh
meson setup builddir && cd builddir
meson compile
```

## Using the library

From the build folder, you can directly import the library and use it:

```python
import pyarrow as pa

import pyarrow_ext

arr = pa.array([1, 2, 3])
pyarrow_ext.sum(arr)
```
