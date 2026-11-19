"""Functional probes for execution profiles that imply more than an executable."""
import json
import os
import platform
import subprocess
import sys


def pe64_available():
    """Whether this worker can directly load x86-64 PE executables."""
    architecture = os.environ.get('PROCESSOR_ARCHITEW6432') or os.environ.get('PROCESSOR_ARCHITECTURE', '')
    return sys.platform == 'win32' and architecture.lower() == 'amd64'


def probe_onnx_int8():
    """Check the CPU integer inference contract with signed values and offsets."""
    if sys.platform == 'win32' and hasattr(platform, '_wmi_query'):
        # Use CPython's native platform fallback during package imports.
        def native_platform_only(*args, **kwargs):
            raise OSError('Use native Windows platform discovery')

        platform._wmi_query = native_platform_only
    import cv2
    import numpy as np
    import onnxruntime as ort
    from onnx import TensorProto as types, helper, numpy_helper

    shape = [1, 1, 1, 2]
    constants = {
        'w': np.array([[[[126]]]], dtype=np.uint8),
        'zero': np.array(128, dtype=np.uint8),
    }
    graph = helper.make_graph(
        [helper.make_node('ConvInteger', ['x', 'w', 'zero', 'zero'], ['y'])],
        'integer_convolution', [helper.make_tensor_value_info('x', types.UINT8, shape)],
        [helper.make_tensor_value_info('y', types.INT32, shape)],
        [numpy_helper.from_array(value, name) for name, value in constants.items()],
    )
    model = helper.make_model(graph, opset_imports=[helper.make_opsetid('', 13)], ir_version=8)
    options = ort.SessionOptions()
    options.intra_op_num_threads = options.inter_op_num_threads = 1
    options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    session = ort.InferenceSession(model.SerializeToString(), sess_options=options,
                                   providers=['CPUExecutionProvider'])
    result = session.run(None, {'x': np.array([[[[130, 126]]]], dtype=np.uint8)})[0]
    expected = np.array([[[[-4, 4]]]], dtype=np.int32)
    image = cv2.cvtColor(np.zeros((2, 2, 3), dtype=np.uint8), cv2.COLOR_BGR2GRAY)
    if not np.array_equal(result, expected) or image.shape != (2, 2):
        raise ValueError('CPU integer inference produced unexpected results')
    return {'runtime': ort.__version__, 'provider': session.get_providers()[0], 'result': result.ravel().tolist()}


def check_onnx_int8(argv, env):
    try:
        result = subprocess.run([*argv, '-B', __file__], env=env, capture_output=True,
                                timeout=20, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        if result.returncode == 0:
            return json.loads(result.stdout)
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return None


if __name__ == '__main__':
    print(json.dumps(probe_onnx_int8()))
