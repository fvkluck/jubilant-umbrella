(ns networksim.main
  (:require [hyperfiddle.rcf :as rcf]))

(comment
  (rcf/enable!))

(defonce logs (atom []))

(defn log [msg]
  (swap! logs conj msg))

(defn show-log []
  @logs)

(comment
  (show-log))

(defn clear-log []
  (reset! logs []))

(rcf/tests (def graph
             {:nodes #{:u :v :w :x :y :z}
              :links {#{:u :v} 4
                      #{:u :w} 7
                      #{:u :x} 9
                      #{:v :x} 4
                      #{:v :w} 3
                      #{:w :x} 2
                      #{:w :y} 8
                      #{:x :y} 4
                      #{:w :z} 3
                      #{:y :z} 6}}))

(defn watch-agent [n]
  (let [watch-fn (fn [k _reference _old new-state]
                   (log (str k new-state)))]
    (add-watch n (str "watch-" (:id @n)) watch-fn)))

(defn make-node [data]
  (-> (agent (select-keys data [:id :neighbours :routes]))
      (watch-agent)))

(defn distance [link node]
  (let [[pair d] link]
    (if (pair node)
      (let [neighbour (-> pair
                          (disj node)
                          first)]
        [neighbour d]))))

(rcf/tests
  (distance [#{:u :v} 4] :u) := [:v 4]
  (distance [#{:u :v} 4] :y) := nil)

(defn neighbours [{:keys [links]} node]
  (->> links
       (map #(distance % node))
       (into (hash-map))))

(rcf/tests
  (neighbours graph :u) := {:v 4 :x 9 :w 7}
  (neighbours graph :z) := {:w 3  :y 6}
  (neighbours {:nodes #{} :links #{}} :u) := {}
  (neighbours graph :nan) := {})

(defn add-node [network graph node-id]
  (let [nbs-in-network (->> (neighbours graph node-id)
                            (filter (fn [[k _v]] (k network)))
                            (into {}))]
    (-> network
        (assoc node-id (make-node {:id node-id
                                   :neighbours nbs-in-network})))))

(rcf/tests
  (defn- network-state
    "Helper function to get current state of nodes in network"
    [network]
    (->> (seq network)
         (map (fn [[k v]] [k @v]))
         (into {})))

  (network-state (add-node {} graph :x)) := {:x {:id :x
                                                 :neighbours {}}}
  (network-state (-> {}
                     (add-node graph :x)
                     (add-node graph :u))) := {:x {:id :x
                                                   :neighbours {:u 9}
                                                   :routes {:u {:path '()
                                                                :distance 9}}}
                                               :u {:id :u
                                                   :neighbours {:x 9}
                                                   :routes {:x {:path '()
                                                                :distance 9}}}}
  )

(defn build-network [{:keys [nodes _links] :as graph}]
  (into {} (for [node nodes]
    [node (make-node {:id node
                      :neighbours (neighbours graph node)
                      :routes {}})])))

(rcf/tests (def network (build-network graph)))


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

(rcf/tests
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
