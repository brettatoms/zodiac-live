(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.github.brettatoms/zodiac-live)

(defn- version-base [patch] (format "0.1.%s" patch))
(def version (version-base (b/git-count-revs nil))) ;; git rev-list --count HEAD
(def snapshot (version-base "9999-SNAPSHOT"))
(def class-dir "target/classes")

;; delay to defer side effects (artifact downloads)
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn test "Run all the tests." [opts]
  (b/process {:command-args ["clojure" "-M:test"]})
  (b/process {:command-args ["clojure" "-M:clj-kondo" "--lint" "src" "test"]})
  (b/process {:command-args ["clojure" "-M:cljfmt" "check"]})
  opts)

(defn- pom-template [version]
  [[:description "Zodiac extension for remuda and darkstar"]
   [:url "https://github.com/brettatoms/zodiac-live"]
   [:licenses
    [:license
     [:name "MIT License"]
     [:url "https://mit-license.org/license.txt"]]]
   [:developers
    [:developer
     [:name "Brett Adams"]]]
   [:scm
    [:url "https://github.com/brettatoms/zodiac-live"]
    [:connection "scm:git:https://github.com/brettatoms/zodiac-live.git"]
    [:developerConnection "scm:git:ssh:git@github.com:brettatoms/zodiac-live.git"]
    [:tag (str "v" version)]]])

(defn- jar-opts [opts]
  (let [version (if (:snapshot opts) snapshot version)]
    (println "\nVersion:" version)
    (assoc opts
           :lib lib   :version version
           :jar-file  (format "target/%s-%s.jar" lib version)
           :basis     (b/create-basis {})
           :class-dir class-dir
           :target    "target"
           :src-dirs  ["src"]
           :pom-data  (pom-template version))))

(defn jar [opts]
  (let [opts (jar-opts opts)]
    ;; Pass the full opts (incl. :pom-data) so the POM carries the license,
    ;; SCM, etc. Clojars rejects a POM without a license.
    (b/write-pom opts)
    (b/copy-dir {:src-dirs ["src"]
                 :target-dir class-dir})
    (b/jar opts)))

(defn install
  "Install the JAR to the local Maven repo.

  Needed because the three libraries depend on each other by released
  coordinate: a downstream repo cannot resolve an upstream version that is not
  published yet, so during development the upstream is installed locally first."
  [opts]
  (jar opts)
  (let [{:keys [jar-file] :as opts} (jar-opts opts)]
    ;; b/install wants a string; b/resolve-path returns a File.
    (b/install (assoc opts :jar-file (str (b/resolve-path jar-file)))))
  opts)

(defn deploy "Deploy the JAR to Clojars." [opts]
  (let [{:keys [jar-file] :as opts} (jar-opts opts)]
    (dd/deploy {:installer :remote
                :artifact (b/resolve-path jar-file)
                :pom-file (b/pom-path (select-keys opts [:lib :class-dir]))}))
  opts)
