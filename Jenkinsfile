/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@Library('jenkins_lib')_
pipeline {
    agent any

  environment {
    // Define global environment variables in this section
    SLACK_CHANNEL = 'jenkins-misc-alerts'
    ARCHIVE_PATH = "target/dists/rpm/*.rpm"
    SONAR_PATH = './'

 
  }
  stages {
    stage("Define Release version"){
      steps {
      script {
       //Global Lib for Environment Versions Definition
        versionDefine('pom.xml')
        }
      }
    }
    stage("Compile, Build and Test") {
      steps {
      script {
        echo "Running Build and Test"
        sh 'sh start-build-env.sh'
        sh 'mvn -B clean install rpm:rpm -DnewVersion=2.7.3.0.0 -DbuildNumber=4295bb16c439cbc8fb0e7362f19768dde1477868 -DskipTests -Dpython.ver="python >= 2.6"'
             }
      }
    }
    stage('SonarQube analysis') {
    steps {
      script {
        //Global Lib for Sonarqube runnner JAVA
        sonarqube(env.SONAR_PATH)
      }
    }
    }
stage("RPM Collect"){
    steps {
      script {
          sh "mkdir rpms"
          sh "find ./ -name *.rpm -exec cp -n {} rpms/ ||true"
      }
    }
    }
stage("RPM Push"){
    steps {
      script {
       rpm_push ( 'release', 'rpms/', 'ggn-dev-rpms/ambari')
      }
    }
    }
    
 
}
  post {
       always {
 
          postBuild(env.ARCHIVE_PATH)
         //Global Lib for post build actions eg: artifacts archive
 
          slackalert(env.SLACK_CHANNEL)
         //Global Lib for slack alerts
 
      }
    }
}
