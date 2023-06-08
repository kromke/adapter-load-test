(ns adapter-load-test.core
  (:require [dvlopt.kafka       :as K]
            [dvlopt.kafka.out   :as K.out]
            [dvlopt.kafka.in   :as K.in]
            [clojure.data.json :as json])
  (:import [java.time Instant Duration])
  (:gen-class))

(defn get-config [file] (read-string (slurp file)))

(defn send-messages [conf load]
  (let [{:keys [kafka-servers topic headers]} conf
        value (json/write-str (:value conf))
        {:keys [requestType requestId traceKey] :or {requestType ""}} headers
        requestId* (atom [])
        ]

    (with-open [producer (K.out/producer {::K/nodes kafka-servers
                                          ::K/serializer.key :string
                                          ::K/serializer.value :string
                                          ::K.out/configuration {"client.id" "my-producer"}})]
      (dotimes [_ load]
        (K.out/send producer
                    {::K/topic topic
                     ::K/headers {"requestType" (.getBytes requestType)
                                  "requestId" (.getBytes (last (swap! requestId* conj (eval requestId))))
                                  "traceKey" (.getBytes (eval traceKey))}
                     ::K/value value}
                    (fn callback [exception _]
                                       (when exception
                                         "FAILURE"
                                         ))))) @requestId*))

(defn consume-messages [conf ids]
  (let [{:keys [kafka-servers response-topic]} conf
        a-ids (atom ids)]
    (with-open [consumer (K.in/consumer {::K/nodes              kafka-servers
                                         ::K/deserializer.key   :string
                                         ::K/deserializer.value :string
                                         ::K.in/configuration   {"group.id"           "test-group"}})]
      (K.in/register-for consumer
                         [response-topic])
      (while (not-empty @a-ids) (doseq [record (K.in/poll consumer
                                                          {::K/timeout [5 :seconds]})]
                                  (let [id (:requestId
                                            (reduce
                                             (fn [m [k v]] (assoc m (keyword k) (String. v)))
                                             {}
                                             (::K/headers record)))]
                                    (swap! a-ids (fn [coll id] (remove #(= id %) coll)) id)
                                    (println (format "Record %s" id))))))))

(defn test-kafka
  ([conf] (test-kafka conf (:message-number conf)))
  ([conf load] (let [start-time (atom "")]

                 (->> (send-messages conf load)
                      (do (reset! start-time (Instant/now)))
                      (consume-messages conf))

                 (let [end-time (Instant/now)
                       duration (Duration/between @start-time end-time)
                       result-str (format "
======================================
load  %d
start %s
end   %s
diff  %d sec
diff  %d min"
                                          load @start-time end-time
                                          (.getSeconds duration)
                                          (quot (.getSeconds duration) 60))]

                   (println result-str)
                   (spit "result.txt" result-str :append true)))))

(defn -main [& args]
  (let [conf (get-config (first args))]
    (if-let [load (second args)]
      (dotimes [n (Integer/parseInt load)] (test-kafka conf (int (Math/pow 10 n))))
      (test-kafka args)))
  (println "The end..."))

(comment
  (-main "/home/kromke/Документы/ЕГРН 2/tests-doc/sa-bft-dev.edn" "6")
  (send-messages (get-config "/home/kromke/Документы/ЕГРН 2/tests-doc/sa-bft-dev.edn") 2))
