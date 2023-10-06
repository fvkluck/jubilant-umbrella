(ns main
  (:require [hyperfiddle.rcf :as rcf]
            [clojure.core.async :refer [go <! >!  <!! chan >!! close!] :as async]))

(comment
  (hyperfiddle.rcf/enable!))

(defonce logs (atom []))

(defn log [msg]
  (swap! logs conj msg))

(defn show-log []
  @logs)

(comment
  (show-log))

(defn clear-log []
  (reset! logs []))

(def graph
  {:nodes #{:u :v :w :x :y :z}
   :links {[:u :v] 4
           [:u :w] 7
           [:u :x] 9
           [:v :x] 4
           [:v :w] 3
           [:w :x] 2
           [:w :y] 8
           [:x :y] 4
           [:w :z] 3
           [:y :z] 6}})

(defn watch-agent [n]
  (let [watch-fn (fn [k _reference _old new-state]
                   (log (str k new-state)))]
    (add-watch n (str "watch-" (:id @n)) watch-fn)))

(defn make-node [data]
  (-> (agent (select-keys data [:id :neighbours :routes]))
      (watch-agent)))

(defn distance [link node]
  (let [[[n1 n2] d] link]
    (cond
      (= node n1) [n2 d]
      (= node n2) [n1 d]
      :else nil)))

(hyperfiddle.rcf/tests
  (distance [[:u :v] 4] :u) := [:v 4]
  (distance [[:u :v] 4] :y) := nil)

(defn neighbours [{:keys [links]} node]
  (->> links
       (map #(distance % node))
       (into (hash-map))))

(hyperfiddle.rcf/tests
  (neighbours graph :u) := {:v 4 :x 9 :w 7}
  (neighbours graph :z) := {:w 3  :y 6}
  (neighbours {:nodes #{} :links #{}} :u) := {}
  (neighbours graph :nan) := {})

(defn worker [{{:keys [in out distance]} :conn :keys [id message-handler]}]
  (atom {:id id
         :message-handler message-handler})
  )

(defn make-connection [d]
  {:in (chan)
   :out (chan)
   :distance d})

(defn reverse-connection [{:keys [in out] :as conn}]
  (-> conn
      (assoc :in out)
      (assoc :out in)))

(defn add-connection [w conn-id conn]
  (update w :neighbours assoc conn-id conn))

(defn connect-workers! [w1 w2 d]
  (let [conn (make-connection d)]
    (swap! w1 add-connection (:id @w2) conn)
    (swap! w2 add-connection (:id @w1) (reverse-connection conn))))

(rcf/tests
  (def wx (atom {:id :x}))
  (def wy (atom {:id :y}))
  (connect-workers! wx wy 3)
  (nil? (:x (:neighbours @wy))) := false
  (nil? (:y (:neighbours @wx))) := false)

(defn set-message-handler
  "Set the message-handler of worker w to f. f should be a function [w msg] that accepts a worker
  and a msg, and returns the updated worker"
  [w f]
  (assoc w :message-handler f))

(defmulti handle-message (fn [w msg] (:id msg)))

(defmethod handle-message :greet
  [w msg]
  (log msg)
  (println "Greeting")
  w)

(defmethod handle-message nil
  [w msg]
  (log msg)
  (println "Worker" (:id w) "received message:" msg)
  w)

(defn listen-neighbour! [worker nb-id]
  (go
    (loop []
      (let [in-chan (-> @worker :neighbours nb-id :in)
            msg (<! in-chan)]
        (if (= msg :disconnect)
          (do
            (println "Closing")
            (close! in-chan)
            (swap! worker update :neighbours dissoc nb-id))
          (do (when-let [handler (:message-handler @worker)]
                (swap! worker handler msg))
              (recur)))))))

(rcf/tests
  (def test-chan (chan 1))
  (defn test-handler [w msg]
    (>!! test-chan msg)
    w)
  (def in (chan))
  (def out (chan))
  (def w (worker {:id :x
                  :message-handler test-handler}))
  (swap! w add-connection :u {:in in :out out :distance 4})
  (listen-neighbour! w :u)
  (>!! in "Hello")
  (<!! test-chan) := "Hello"

  (>!! in :disconnect)
  (<!! (async/timeout 100))
  (:neighbours @w) := {}
  )

(defn build-network [{:keys [nodes] :as _graph}]
  (into {} (for [n nodes]
             [n (worker {:id n
                         :message-handler handle-message})])))

(defn start-system
  [{:keys [links] :as graph}]
  (let [network (build-network graph)]
    (doseq [[[n1 n2] distance] links]
      (connect-workers! (n1 network) (n2 network) distance)
      (listen-neighbour! (n1 network) n2)
      (listen-neighbour! (n2 network) n2))
    network))

(comment
  (def network (start-system graph)))

(comment "greet neighbours"
         (let [w (:x network)]
           (for [[n-id {:keys [in out d]}] (:neighbours @w)]
             (go (>! out :greet)))))

(comment (let [in (chan)
               out (chan)
               w (-> (worker {:id :x
                              :conn {:in in :out out :distance 4}
                              :message-handler handle-message}))]
           (listen-neighbour! w :u)
           (>!! in "Hello")
           @w
           (>!! in {:id :greet})
           @w
           (>!! in :disconnect)
           (<!! (async/timeout 100))
           @w))



#_(defn build-network [{:keys [nodes _links] :as graph}]
  (into {} (for [node nodes]
    [node (make-node {:id node
                      :neighbours (neighbours graph node)
                      :routes {}})])))


; example node
{:id :u
 :neighbours {:v 4 :x 9 :w 7}
 :routes {:v [{:path '()
               :distance 4}
              {:path '(:x)
               :distance 13}]}
 :requests {:z [:y :x]} ; :y and :x are looking for a route to :z
 }

(defn shortest-route [self route-target]
  (let [routes (-> self :routes route-target)]
    (apply min-key :distance routes)))

(defn update-route [self source route-target {:keys [path distance] :as _route}]
  (update-in self [:routes route-target]
             (fnil conj [])
             {:path (conj path source)
              :distance (+ distance (-> self :neighbours source))}))  ; sender knows the distance, not receiver

(hyperfiddle.rcf/tests
  (:routes (update-route {:id :u
                          :neighbours {:v 4 :x 9 :w 7}
                          :routes {:v [{:path (list)
                                        :distance 4}]}}
                         :x
                         :v
                         {:path (list)
                          :distance 4})) := {:v [{:path '()
                                                  :distance 4}
                                                 {:path '(:x)
                                                  :distance 13}]}
 (:routes (update-route {:id :u
                          :neighbours {:v 4 :x 9 :w 7}
                          :routes {}}
                         :x
                         :v
                         {:path (list)
                          :distance 4})) := {:v [{:path (list :x)
                                                  :distance 13}]}
  )

(declare handle-message)
(defn notify-neighbours! [self route-target]
  (doseq [n (-> self :requests route-target)]
    (handle-message :update-route n (:id self) route-target (shortest-route self route-target))))

(defn handle-update-route [self source route-target {:keys [_path _distance] :as route}]
  (let [new-node (-> self
                     (update-route source route-target route))]
    (notify-neighbours! self route-target)
    new-node))


(comment
  ; some useful commands
  (agent-errors (:u network))

  (send (:u network) assoc :requests {:v [:w]})
  (handle-message :update-route :u :x :v {:path (list)
                                        :distance 4})
  )

(def network (build-network graph))

(defmulti handle-message (fn [msg-id & _] msg-id))

(defmethod handle-message :update-route [_ receiver & args]
  (apply (partial send (receiver network) handle-update-route) args))


#_(defmethod handle-message :notify-neighbours [_ receiver & args]
    (apply (partial send (receiver network) notify-neighbours!) args))

; example messages, not yet how they're currently implemented
{:header {:addressee :y
          :source :x
          :path (list)}
 :body {:message-type :update-route
        :target :u
        :path (list :y :z)
        :distance 10}}

{:header {:source :x
          :addressee :y
          :path (list)}
 :body {:message-type :update-route
        :target :u}}
