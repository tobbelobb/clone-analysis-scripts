; (ql:quickload :split-sequence)
; (ql:quickload :drakma)
; (ql:quickload :cl-json)

(defpackage #:my-cl-neo4j
  (:nicknames :my-neo4j)
  (:use #:cl)
  (:import-from :drakma
                :http-request)
  (:import-from :flexi-streams
                :octets-to-string
                :flexi-stream-external-format
                :peek-byte)
  (:import-from :cl-json
                :*json-output*
                :encode-json
                :encode-json-to-string
                :encode-json-alist-to-string
                :decode-json-from-string
                :json-bind
                :decode-json)
  (:import-from :split-sequence
                :split-sequence)
  (:documentation "A RESTful api to Neo4j that I know and understand myself.~
                   Not intended for usingn stand-alone. Just for building the ~
                   pisf-backend.")
  (:export ;; Functions from my-cl-neo4j. No by-id kind of functions
           #:bind-cypher-returns-lists
           #:bind-cypher-returns-singles
           #:bind-cypher-returns-and-loop
           #:neo-request
           #:transaction-request
           #:abort-transaction ; macro
           #:cypher-query
           #:get-node
           #:delete-node
           #:get-names-of-nodes
           #:get-rel-types
           #:get-property-keys
           ;#:get-indexes ; "legacy"... pff...
           ;; Utils
           #:lispify-output ; macro
           #:row-data-from-transaction-call
           #:ca
           ;; Managing database (from config.lisp)
           #:neo4j-executable
           #:get-constraints
           #:number-of-nodes-in-db
           #:number-of-rels-in-db
           #:database-empty?
           #:probe-database
           #:update-neo4j-uris
           ;; Neo4j-constants
           #:*neo4j-executable-path*
           #:*host*
           #:*data*
           #:*node*
           #:*node-index* ; What does node-index do?
           #:*rel-index*
           #:*rel-types*
           #:*batch*
           #:*cypher*
           #:*indexes*
           #:*constraints*
           #:*transaction*
           #:*labels*
           #:*neo4j-version*
           #:*rel*
;           ;; Logging constants
;           #:*log-level*
;           #:*neo4j-log-stream*
;           ;; Loggin functions/macros
;           #:set-log-level ; :info, :debug, :warn, :error or :none
;           #:get-log-level
;           #:lg ; like format, but only outputs when level >= *log-level*, and only into *neo4j-log-stream*
           ))

(in-package :my-neo4j)

(defmacro aif (test-form then-form &optional else-form)
  `(let ((it ,test-form))
     (if it ,then-form ,else-form)))

(defmacro awhen (test-form &body body)
  `(aif ,test-form
        (progn ,@body)))

;; Parses octet-arrays from neo4j.
;; A bit slow?
(defmacro lispify-output (form)
  (let ((resp (gensym "CONTENTS-"))
        (resp-code (gensym "CODE-")))
    `(multiple-value-bind (,resp ,resp-code) ,form
      (values (my-decode-json-string (octets-to-string ,resp))
              ,resp-code))))

;; used only as helper to bind-cypher-returns macro
(defun query->syms (query)
  (mapcar #'(lambda (str) (intern (string-upcase (string-trim " " str))))
          (split-sequence:split-sequence
            #\,
            (subseq query
                    (+ 7 ; 7 letters in " RETURN"
                       (search "RETURN" query :from-end t)))
            :test #'char=)))

; Why doesn't decode-json-from string handle empty strings by default?
; to do : check it up
(defun my-decode-json-string (s) ; Used by lispify-output
  (unless (string= s "")
    (decode-json-from-string s)))

(defun ca (item alist)
  (cdr (assoc item alist)))

(defun row-data-from-transaction-call (from-neo)
  (mapcar #'cdar (ca :data (car (ca :results from-neo)))))

;; Should have been used instead:
;(defmacro with-neo4j-response-vars (varlist &body body)
;  `(json-bind ,varlist

;; Supposed to collect values from all key-value-pairs where
;; key matches var. I would rather have this than lispify-output+assoc
;; (defmacro json-bind-multiple ((&rest vars) json-source &body body)

;; Should roll my own json-encoder...

;; Used in transaction request
(defun statements-json-list (alist)
  (format nil "{\"statements\" : [~a]}"
          (encode-json-to-string alist)))

;; logging
(defvar *log-level* 0)

(defparameter *log-levels* '(:info :debug :warn :error :none))

(defparameter *neo4j-log-stream* *standard-output*)

(defun set-log-level (level)
  (awhen (position level *log-levels*)
    (setq *log-level* it)
    level))

(defun get-log-level ()
  (nth *log-level* *log-levels*))

(defmacro lg (level str &rest args)
  (let ((n (position level *log-levels*)))
    (when n
      `(when (>= ,n *log-level*)
	        (format *neo4j-log-stream* ,(concatenate 'string "MY-NEO4j: " str) ,@args)))))

;; Config

(defparameter *host* "http://localhost:7474/")
;; For https requests:
;; (defparameter *host* "http://localhost:7473/")


;; Convenience functions and variables based on *host* value
(defun add-host (request-uri)
  (concatenate 'string *host* request-uri))

(defparameter *data* (add-host "db/data"))
(defun add-data (s)
  (concatenate 'string *data* s))
;; The following uris can be updated through Neo4j's REST API with
;; (get-service-root)
(defparameter *node* (add-data "/node"))
(defparameter *node-index* (add-data "/index/node"))
(defparameter *rel-index* (add-data "/index/relationship"))
(defparameter *rel-types* (add-data "/relationship/types"))
(defparameter *batch* (add-data "/batch"))
(defparameter *cypher* (add-data "/cypher"))
(defparameter *indexes* (add-data "/schema/indexes"))
(defparameter *constraints* (add-data "/schema/constraint"))
(defparameter *transaction* (add-data "/transaction"))
(defparameter *labels* (add-data "/labels"))
(defparameter *neo4j-version* "Unknown")
;; Interface doesn't give any relationship request-uri... Why?
(defparameter *rel* (add-data "/relationship"))

;; When POST'ed to its db/data request uri, neo4j responds with all its other
;; request uris. We use the ones Neo4j provides itself rather than hard coding
;; what's in the current REST API manual.
(defun get-service-root ()
  (lispify-output (http-request *data*))) ; Can't run if neo4j isn't running...

;; Sould definitely run this from some sort of init-pisf-backend
(defun update-neo4j-uris ()
  (let ((service-root-answer (get-service-root)))
    (setf *node* (ca :node service-root-answer))
    (setf *node-index* (ca :node--index service-root-answer))
    (setf *rel-index* (ca :relationship--index service-root-answer))
    (setf *rel-types* (ca :relationship--types service-root-answer))
    (setf *batch* (ca :batch service-root-answer))
    (setf *cypher* (ca :cypher service-root-answer))
    (setf *indexes* (ca :indexes service-root-answer))
    (setf *constraints* (ca :constraints service-root-answer))
    (setf *transaction* (ca :transaction service-root-answer))
    (setf *labels* (ca :node--labels service-root-answer))
    (setf *neo4j-version* (ca :neo-4-j--version service-root-answer))))

(defun got-cypher-data (cypher-ret-val)
  (ca :data cypher-ret-val))

(defmacro bind-cypher-returns-lists ((query &key params format-args) &body body)
  (let ((cypher-ret-val (gensym "RET-VAL-"))
        (formatted-query (apply #'format nil query format-args)))
    `(let* ,(append
               `((,cypher-ret-val (cypher-query ,formatted-query
                                               :params ,params)))
               (loop for sym in (query->syms formatted-query)
                     and x = 0 then (1+ x)
                     collect `(,sym (mapcar #'(lambda (l) (elt l ,x))
                                            (ca :data ,cypher-ret-val)))))
     (when (got-cypher-data ,cypher-ret-val) ,@body))))

(defmacro bind-cypher-returns-singles ((query &key params format-args) &body body)
  (let ((cypher-ret-val (gensym "RET-VAL-"))
        (formatted-query (apply #'format nil query format-args)))
    `(let* ,(append
               `((,cypher-ret-val (cypher-query ,formatted-query
                                               :params ,params)))
               (loop for sym in (query->syms formatted-query)
                     and x = 0 then (1+ x)
                     collect `(,sym (car (mapcar #'(lambda (l) (elt l ,x))
                                            (ca :data ,cypher-ret-val))))))
     (when (got-cypher-data ,cypher-ret-val) ,@body))))

(defmacro bind-cypher-returns-and-loop ((query &key params format-args) &body body)
  (let* ((formatted-query (apply #'format nil query format-args))
         (syms (query->syms formatted-query)))
    `(let ((ret-lists (ca :data (cypher-query ,formatted-query :params ,params))))
       (when ret-lists
         (do* ,(append
                 `((ret-list (pop ret-lists) (pop ret-lists)))
                 (loop for sym in syms and x = 0 then (1+ x)
                       collect `(,sym (pop ret-list) (pop ret-list))))
           ((null ,(first syms)))
           ,@body)))))

;;(macroexpand-1 '(bind-cypher-returns-and-loop ("MATCH (ma:Mini_article) RETURN ma.name, ma.title")
;;                  (format t "~a~%" ma.name)
;;                  (format t "~a~%" ma.title)))
;;
;;

;; Handling neo4j's REST API starts here

; Raw http requests to neo4j server.
; Directly from REST API manual
; adds content-type, accept, and X-Stream : true header to all calls
; Not 100% necessary, but easiest on the server, according to REST API manual
; Dont know if :want-stream and X-stream is worth it or even working.
; bmVvNGo6T2JpanVhbg== is base64 encoded neo4j:obijuan
(defun neo-request (uri &optional &key (decode nil) (method :get) (payload nil) (auth "Basic bmVvNGo6b2JpanVhbg=="))
  (lg :info "Sending request to ~a~%" uri)
  (multiple-value-bind (in-stream return-code #2=#:ignore #3=#:ignore #4=#:ignore must-close
                                  reason-string)
    (http-request uri :method method
                  :content-type "application/json"
                  :accept "application/json"
                  :external-format-out :utf-8
                  :external-format-in :utf-8
                  :content (encode-json-to-string payload) ; Don't know if Drakma sends "JSON stream"?
                  :want-stream t      ;X-Stream tells Neo if we're sending a "JSON stream"....
                  :additional-headers `(("X-stream" . ,(if payload "true" "false"))
                                        ("Authorization" . ,auth)))
    (declare (ignore #2#))
    (declare (ignore #3#))
    (declare (ignore #4#))
    (setf (flexi-stream-external-format in-stream) :utf-8)
    (let ((ret-val (when (and decode
                              (not (eq 'eof (peek-byte in-stream nil nil 'eof))))
                     (decode-json in-stream))))
      (when must-close (close in-stream))
      (values ret-val return-code reason-string))))

(defun transaction-request (statement parameters &key (commit-uri nil) (commit? nil))
  (let ((content (statements-json-list
                 `(("statement" . ,statement)
                   ("parameters" . ,parameters)))))
  (lispify-output
    (http-request
      (cond ; Deciding which uri to send to
        ((and commit-uri commit?)
          (progn (lg :debug "Commiting transaction ~a.~%~
                             Content: ~a~%" commit-uri content) commit-uri))
        (commit-uri
          (progn (lg :debug "Adding to transaction ~a~%~
                             Content: ~a~%" (string-right-trim "/commit" commit-uri) content)
                 (string-right-trim "/commit" commit-uri)))
        (commit?
          (progn (lg :debug "Creating and commiting transaction~%~
                             Content: ~a~%" content)
                 (format nil "~a/~a" *transaction* "commit"))) ; Commit transaction directly...
        (t (progn (lg :debug "Initiating transaction.~%~
                              Content: ~a~%" content)
                  *transaction*)))
      :method :post
      :content-type "application/json"
      :content content))))

;; This is a macro just because of ,@format-args
;; How to do that with a function?..
(defmacro abort-transaction (commit-uri message &rest format-args)
  `(progn (http-request (string-right-trim "commit" ,commit-uri) :method :delete) ; deletes the transaction
          (lg :warn "Aborting transaction ~a~%" ,commit-uri)
          (lg :error ,message ,@format-args)))

;; payload is an alist ("looks" . "funny")
;; Returns (values body status) from HTTP-REQUEST
(defun create-node (&optional &key (payload nil) (decode nil))
  (neo-request *node* :method :post
                      :payload payload
                      :decode decode))

;; To create multiple nodes with props
;; (cypher-query "CREATE (n:Person {props}) RETURN n"
;;               '((props .
;;                 #((("name" . "Andres") ("position" . "Developer"))
;;                   (("name" . "Peter") ("position"  . "Developer"))))))
(defun cypher-query (query &key (params ()) (decode t))
  (lg :info "Cypher query: ~a~%sent with params ~a~%" query params)
  (multiple-value-bind (ret-val ret-code reason)
    (neo-request *cypher*
                 :method :post
                 :payload `(("query" . ,query)
                            ("params" . ,params))
                 :decode decode)
    (when (not (ca :data ret-val))
      (lg :warn "Cypher-query didn't return any data~%"))
    (values ret-val ret-code reason)))


(defun simple-query (label name do-with-node)
  (cypher-query
    (format nil "MATCH (a:~a {name:{name}}) ~a a" label do-with-node)
    `(("name" . ,name))))

(defun get-node (label name)
  (simple-query label name "RETURN"))

(defun delete-node (label name)
  (simple-query label name "DELETE"))

(defun by-id (id &key (method :get) (node-or-rel *node*) (decode nil))
  (neo-request (format nil "~a/~a" node-or-rel id)
                :decode decode
                :method method))

; users of by-id mainly for resemblence with normal English language
(defun get-node-by-id (id &key (decode t))
  (by-id id
         :node-or-rel *node*
         :decode decode))

(defun delete-node-by-id (id)
  (by-id id
         :method :delete
         :node-or-rel *node*))

;; The /db/data/relationship request uri (*rel*), not given by
;; (get-service-root) is used here
(defun get-rel-by-id (id &key (decode t))
  (by-id id
         :node-or-rel *rel*
         :decode decode))

;; Simply returned nil and did nothing, when asked to delete a relationhip
;; that was one of four exactly identical ones. (id 49 from Slic3r to Slicer)
;; Cypher matched all the four rels and deleted them. More reliable.
;; Must check how duplicate rels got there in first place
(defun delete-rel-by-id (id)
  (by-id id
         :method :delete
         :node-or-rel *rel*))

(defun get-properties-for-node-by-id (id &key (decode t))
  (neo-request (format nil "~a/~a/properties" *node* id)
               :decode decode))

(defun get-rels-of-node-by-id (id &key (direction "all") (rel-type "") (decode t))
  ;; :direction can be "in", "out" or "all".
  ;; :rel-type can be several rel-types separated with &
  ;; ampersand may be necessary to encode like %26
    (neo-request (format nil "~a/~a/relationships/~a/~a" *node* id direction rel-type)
                 :decode decode))

;; Returns all known relation types, not only the ones in current use
(defun get-rel-types (&key (decode t))
  (neo-request *rel-types*
               :decode decode))

(defun create-rel-by-ids (from-id to-id rel-type &key (payload nil) (decode nil))
  (neo-request (format nil "~a/~a/relationships" *node* from-id)
                :method :post
                :payload
                `(("to" .  ,(format nil "~a/~a" *node* to-id))
                  ("type" . ,rel-type)
                  ("data" . ,payload))
                :decode decode))

;; Setting property to t becomes true in database
(defun set-property-by-id (id key value)
  (neo-request (format nil "~a/~a/properties/~a" *node* id key)
                :method :put
                :payload value))

(defun delete-all-properties-by-id (id) ;doesn't return json
  (neo-request (format nil "~a/~a/properties" *node* id)
                :method :delete))

(defun get-constraints (&key (decode t))
  (neo-request *constraints*
               :decode decode))

(defun get-id-of-nodes (&key (limit 10) (decode t))
  (mapcar #'car
          (ca :data (cypher-query "MATCH a RETURN id(a) LIMIT {lim}"
                                          :params `(("lim" . ,limit))
                                          :decode decode))))

(defun get-names-of-nodes (&key (limit 10) (decode t) (label nil))
  (mapcar #'car ;this is awkward, but depends on the way Neo returns data in rows and columns
          (ca :data (cypher-query
                      (if label
                        (format nil "MATCH (a:~a) RETURN a.name LIMIT {lim}" label)
                        "MATCH (a) RETURN a.name LIMIT {lim}")
                      :params `(("lim" . ,limit))
                      :decode decode))))

;; Does this not work because it is "legacy indexing"?
(defun get-indexes () ; will not return uniqueness constraints
  (neo-request *node-index* :decode t))

(defun get-property-keys ()
  (neo-request (format nil "~a/~a" *data* "propertykeys") :decode t))

(defun number-of-nodes-in-db () ; This should be known somwhere internal in neo4j...
  (car (car (ca :data (cypher-query "MATCH a RETURN count(a)")))))

(defun number-of-rels-in-db ()
  (car (car (ca :data (cypher-query "MATCH a-[r]->b RETURN count(r)")))))

(defun database-empty? ()
  (not (get-id-of-nodes :limit 1 :decode t)))

(defun probe-database ()
  (let ((response-code (nth-value 1 (http-request *host*))))
    (case response-code
      (200       (format t "Neo4j running and responding"))
      (otherwise (format t "PROBE-DATABASE got response code: ~a ~
                             from host: ~a" response-code *host*)))))

;; Should be able to do this in one transaction with a transaction-request...
;; Rewrite later
(defun delete-node-and-its-relationships-by-id (id &key (decode nil))
  (let ((rel-uris (loop for rel in (get-rels-of-node-by-id id)
                        collect (cdr (assoc :self rel)))))
    (neo-request *batch*
                 :method :post
                 :decode decode
                 :payload
                 (do*
                   ((i (length rel-uris) (1- i))
                    (result `((("method" . "DELETE")
                               ("to" . ,(format nil "~a/~a" *node* id))
                               ("id" . ,(length rel-uris))))
                            (cons `(("method" . "DELETE")
                                    ("to" . ,(car rel-uris))
                                    ("id" . ,i)) result))
                    ; creating inner local copy of rel-uris
                    (rel-uris rel-uris (cdr rel-uris)))
                   ((null rel-uris) result)))))

