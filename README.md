# data streaming & stream analytics
> For deployment steps follow Deployment.md and for details about the project raed Report.md

> Below is the project delivery directory structure. 

```
.
├── LICENSE
├── README.md
├── code
│   ├── README.md
│   └── mysimbdp
│       ├── docker-compose.yaml
│       ├── mongo
│       │   ├── config.init.js
│       │   ├── enable-shard.js
│       │   ├── router.init.js
│       │   ├── shardrs.init.js
│       │   └── user.init.js
│       ├── mysimbdp-up.sh
│       ├── requirements.txt
│       └── stream-app
│           ├── app.py
│           ├── client-1.py
│           ├── client-2.py
│           ├── client-data-model.yaml
│           ├── client-dataset.yaml
│           └── manager.py
├── data
│   └── client-staging-input-directory
│       ├── client-1.csv
│       ├── client-2.csv
│       └── processed
├── logs
│   ├── stream_log_stream-topic-1.csv
│   └── stream_log_stream-topic-2.csv
└── reports
    ├── deployment.md
    ├── report.md
    └── resources
        ├── analytics-workflow.png
        ├── architecture.png
        ├── batch-workflow.png
        ├── confluent-up.png
        ├── kafka-UI.png
        ├── spark-UI.png
        └── spark-up.png
```