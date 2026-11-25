{{- define "wb-graph.sparkScript" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .scriptName }}
data:
  daily_recipients.py: |
    {{- .root.Files.Get "files/spark/daily_recipients.py" | nindent 4 }}
{{- end }}

{{- define "wb-graph.sparkJob" -}}
{{- $hdfs := printf "hdfs://%s-hdfs-namenode:8020" .root.Release.Name -}}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ .name }}
spec:
  backoffLimit: 1
  activeDeadlineSeconds: 600
  template:
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
        - name: spark
          image: {{ .root.Values.spark.image | quote }}
          imagePullPolicy: IfNotPresent
          command: ["/opt/spark/bin/spark-submit"]
          args:
            - "--master"
            - "local[2]"
            - "--driver-memory"
            - "768m"
            - "--conf"
            - "spark.hadoop.dfs.replication=1"
            - "/job/daily_recipients.py"
            - "--archive"
            - {{ printf "%s%s/%s" $hdfs .root.Values.hdfs.archiveRoot .root.Values.kafka.topic | quote }}
            - "--features"
            - {{ printf "%s%s" $hdfs .root.Values.spark.featureRoot | quote }}
            - "--date"
            - "$(FEATURE_DATE)"
            - "--run-id"
            - "$(FEATURE_RUN_ID)"
          env:
            - {name: FEATURE_DATE, value: {{ .date | quote }}}
            - {name: FEATURE_RUN_ID, value: {{ .runID | quote }}}
            - {name: HADOOP_USER_NAME, value: hadoop}
            - {name: SPARK_LOCAL_IP, value: "127.0.0.1"}
          resources:
            {{- toYaml .root.Values.spark.resources | nindent 12 }}
          volumeMounts:
            - {name: script, mountPath: /job, readOnly: true}
      volumes:
        - name: script
          configMap: {name: {{ .scriptName }}}
{{- end }}
