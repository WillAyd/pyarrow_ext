project(
    'pyarrow_ext',
    'c',
    default_options: [
        'warning_level=2',
        'c_std=c17',
    ],
)

py = import('python').find_installation()
nanoarrow_dep = dependency(
    'nanoarrow',
    default_options: ['default_library=static'],
)

py.extension_module(
    'pyarrow_ext',
    sources: ['src/pyarrow_ext/pyarrow_ext.c'],
    dependencies: [nanoarrow_dep],
)
