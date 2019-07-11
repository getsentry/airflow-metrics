from setuptools import find_packages, setup


setup(
    name='airflow-metrics',
    version='0.1.3',
    author='zylphrex',
    author_email='zylphrex@gmail.com',
    maintainer='zylphrex',
    maintainer_email='zylphrex@gmail.com',
    url='https://github.com/getsentry/airflow-metrics',
    description='Airflow plugin for automatically sending metrics from Airflow to Datadog',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Plugins',
        'License :: OSI Approved :: Apache Software License',
    ],
    platforms=[
        'MacOS',
        'Unix',
        'Windows',
    ],
    keywords=[
        'airflow',
        'datadog'
        'metrics',
        'plugin',
    ],

    packages=find_packages(exclude=['tests', '*.tests', '*.tests.*', 'tests.*']),
    entry_points={
        'airflow.plugins': [
            'airflow_metrics = airflow_metrics:AirflowMetricsPlugin',
        ],
    },
    install_requires=open('requirements.txt').read().split('\n'),
)
