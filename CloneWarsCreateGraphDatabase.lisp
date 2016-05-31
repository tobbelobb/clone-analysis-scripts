;;; Script to make data from CloneWarsDataCollection.lisp
;;;   into a Neo4j graph database that we can work with

;; (ql:quickload :cl-ppcre)
;; (ql:quickload :split-sequence)
;; (ql:quickload :drakma)
;; (ql:quickload :cl-json)
;; (load "./my-cl-neo4j.lisp")
;; Observe: neo4j requires authentization token to accept cypher queries.
;;          Either set "obijuan" without the quotes as your password (and keep default username "neo4j" without quotes)
;;            or enter a new base64 encoded password/username pair into my-cl-neo4j.lisp (ca line 259)

;; Strategy is:
;;  - Read file created by CloneWarsDataCollection.lisp
;;  - Catch printername, madre and username in variables
;;  - Use my-cl-neo4j to do Cypher query
;;  - Get nice graph plot by typing
;;      MATCH (a) -[rel:ABILITYTRANSFER]- (b) RETURN rel
;;    into browser interface
;;  - Get structural virality by Cypher query

(defparameter *the-regex* '(:sequence
                             #\, #\Space
                             (:register
                               (:non-greedy-repetition 1 nil :everything)) ; madre
                             #\, #\Space
                             (:register
                               (:non-greedy-repetition 1 nil :everything)) ; printername
                             #\, #\Space
                             (:register
                               (:greedy-repetition 1 nil :everything)))) ; username

(defun create-graph (filename)
  (with-open-file (filestream filename :direction :input)
    (do ((line (read-line filestream nil) (read-line filestream nil)))
      ((null line))
      ;; for each line
      (cl-ppcre:register-groups-bind (madre pname uname) (*the-regex* line)
          (my-neo4j:cypher-query
            (format nil
                    "MERGE (new_printer:printer {name:{printername}}) ~
                     MERGE (old_printer:printer {name:{madre}}) ~
                     MERGE (new_owner:person {name:{username}}) ~
                     MERGE (old_printer) -[:PRINTED]-> (new_printer) ~
                     MERGE (new_owner)   -[:OWNS]-> (new_printer) ~
                     WITH new_owner, old_printer ~
                     MATCH (helper:person) -[:OWNS]-> (old_printer) ~
                     WHERE helper.name <> new_owner.name ~
                     MERGE (helper) -[:ABILITYTRANSFER]-> (new_owner) ~
                     ")
             :params `(("madre" . ,madre)
                       ("printername" . ,pname)
                       ("username" . ,uname)))))))

;; Cypher query for structural virality of possibly disconnected graph:
;;   MATCH p=shortestPath((n1:person)-[:ABILITYTRANSFER*1..13]-(n2:person))
;;   RETURN toFloat(sum(length(p)))/toFloat(count(p))
;; Increase 13 if needed.
;;
;; Cypher query to get the most active machine part transferers
;;   MATCH (n1:person)-[rel:ABILITYTRANSFER]->(n2:person)
;;   RETURN n1,count(rel) ORDER BY count(rel) DESC
