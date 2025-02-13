(ns core
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [grafter-2.rdf.protocols :as gpr]
            [grafter-2.rdf4j.io :as gio :refer :all]
            [clojure.string :as str]))

(defn read-csv [file-path]
  (with-open [reader (io/reader file-path)]
    (let [rows (csv/read-csv reader)
          headers (map keyword (second rows))
          data (nthrest rows 3)]
      (mapv #(zipmap headers (map (fn [value] (if (clojure.string/blank? value) nil value)) %)) data))))

(def base-uri "http://example.org/")

(def prefixes
  {"rdf" "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   "dct" "http://purl.org/dc/terms/"
   "sdo" "https://schema.org/"
   "wdt" "http://www.wikidata.org/prop/direct/"
   "wd"  "http://www.wikidata.org/entity/"
   "xsd" "http://www.w3.org/2001/XMLSchema#"
   "dcat" "https://www.w3.org/ns/dcat#"
   "eduvocs" "https://eduvocs/"})

(defn make-uri [suffix]
  (java.net.URI. (str base-uri suffix)))

(defn clean-langstring [s]
  (str/replace s #"^\"|\"$" ""))

(def fields
  [
   [:name "https://schema.org/name" #(gpr/->LangString (clean-langstring %) :de)]
   [:title "http://purl.org/dc/terms/title" #(gpr/->LangString (clean-langstring %) :de)]
   [:description "http://purl.org/dc/terms/description" #(gpr/->LangString (clean-langstring %) :de)]
   [:creator "http://purl.org/dc/terms/creator" #(gpr/->LangString (clean-langstring %) :de)]
   [:about "https://schema.org/about" #(gpr/->LangString (clean-langstring %) :de)]
   [:issued "http://purl.org/dc/terms/issued" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#date")]
   [:modified "http://purl.org/dc/terms/modified" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#date")]
   [:language "http://purl.org/dc/terms/language" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#string")] 
   [:license "http://purl.org/dc/terms/license" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#string")]
   [:version "https://schema.org/version" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#string")]
   [:keywords "https://schema.org/keywords" #(gpr/->LangString (clean-langstring %) :de)]
   [:author "http://purl.org/dc/terms/creator" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#string")]
   [:publisher "http://purl.org/dc/terms/publisher" #(gpr/literal % "http://www.w3.org/2001/XMLSchema#string")]
   [:contentUrl "https://schema.org/contentUrl" #(java.net.URI. %)]
   [:url "https://schema.org/url" #(java.net.URI. %)]
   [:described_at "http://www.wikidata.org/prop/direct/P973" #(java.net.URI. %)]
   [:audience "http://purl.org/dc/terms/audience" #(gpr/->LangString (clean-langstring %) :de)]
   [:maintained_by "http://www.wikidata.org/prop/direct/P126" #(gpr/->LangString (clean-langstring %) :de)]
   [:used_by "http://www.wikidata.org/prop/direct/P1535" #(gpr/->LangString (clean-langstring %) :de)]
   [:keywords "https://schema.org/keywords" #(gpr/->LangString (clean-langstring %) :de)]
   [:repo "http://www.wikidata.org/prop/direct/P1324" #(java.net.URI. %)]
   [:endpointUrl "https://www.w3.org/ns/dcat#endpointURL" #(java.net.URI. %)]
   [:educationalLevel "https://schema.org/educationalLevel" #(gpr/->LangString (clean-langstring %) :de)]
   [:rawVocab "https://eduvocs/rawVocab" #(gpr/->LangString (clean-langstring %) :de)]
   [:complies_with "http://www.wikidata.org/prop/direct/P5009" #(gpr/->LangString (clean-langstring %) :de)]
  ]
)

(def dist-lookup-map
  {:json :contentUrlJSON
   :ttl :contentUrlTTL
   :xml :contentUrlXML
   :pdf :contentUrlPDF})

(defn make-distribution-triples [entity-uri row]
  (loop [formats [:json :ttl :xml :pdf]
         triples []]
    (if (empty? formats)
      triples
      (if (clojure.string/blank? (get row (get dist-lookup-map (first formats))))
        (recur (rest formats) triples)
        (let [format (first formats)
              blank-resource (gpr/make-blank-node)
              b-triple (gpr/->Triple
                        blank-resource
                        (java.net.URI. (str (prefixes "sdo") "contentUrl"))
                        (java.net.URI. (get row (get dist-lookup-map format))))
              data-download-triple (gpr/->Triple
                                    blank-resource
                                    (java.net.URI. (str (prefixes "rdf") "type"))
                                    (java.net.URI. (str (prefixes "sdo") "DataDownload")))
              file-format-triple (gpr/->Triple
                                  blank-resource
                                  (java.net.URI. (str (prefixes "sdo") "fileFormat"))
                                  (gpr/literal
                                   (case format
                                     :json "application/json"
                                     :ttl "text/turtle"
                                     :xml "application/xml"
                                     :pdf "application/pdf")
                                   "http://www.w3.org/2001/XMLSchema#string"))
              dist-triple (gpr/->Triple
                           entity-uri
                           (java.net.URI. (str (prefixes "sdo") "distribution"))
                           blank-resource)]
          (recur (rest formats) (conj
                                 triples
                                 b-triple
                                 data-download-triple
                                 file-format-triple
                                 dist-triple)))))))

(defn row-to-triples [row]
  (if (clojure.string/blank? (get row :identifier))
    []
    (let [entity-uri (make-uri (get row :identifier))
          type-triple (gpr/->Triple
                       entity-uri
                       (java.net.URI. (str (prefixes "rdf") "type"))
                       (java.net.URI. (str (prefixes "wd") "Q1469824")))
          distribution-triples (make-distribution-triples entity-uri row)]
      (loop [fields fields
             triples (concat [type-triple] distribution-triples)]
        (if (empty? fields)
          triples
          (let [[field uri tfn] (first fields)
                val (get row field nil)
                new-triples (if (nil? val)
                              triples
                              (conj triples (gpr/->Triple entity-uri (java.net.URI. uri) (tfn val))))]
            (recur (rest fields) new-triples)))))))

(defn csv-to-rdf [file-path]
  (let [rows (read-csv file-path)]
    (mapcat row-to-triples rows)))

(defn write-rdf [triples output-path]
  (with-open [writer (io/writer output-path)]
    (let [tw (gio/rdf-writer  writer {:format :ttl
                                      :prefixes prefixes})]
      (gpr/add tw triples))))

(defn convert-csv-to-rdf [input-csv output-rdf]
  (let [triples (csv-to-rdf input-csv)]
    (write-rdf triples output-rdf)))

(defn -main [& args]
  (convert-csv-to-rdf (first args) (second args)))
