pipeline {
    agent any

    environment {
        REPO_URL = 'https://github.com/Project-Horus-G9/simulador-de-dados'
        DEPLOY_PATH = '/simulador'
        SERVER_USER = 'ubuntu'
        SERVER_HOST = '18.208.141.113'
    }

    triggers {
        githubPush()
    }

    stages {
        stage('Checkout') {
            steps {
                git url: "${REPO_URL}", branch: 'main'
            }
        }

        stage('Install Dependencies') {
            steps {
                script {

		    sh '''sudo apt update
    			sudo apt upgrade -y'''
		    sh 'sudo apt install openjdk-11-jdk -y'

                    if (fileExists('requirements.txt')) {
                        sh 'pip3 install -r requirements.txt'
                    }
                }
            }
        }

        stage('Deploy') {
            steps {
                sshagent(['aws-ssh-key']) {
                    script {
        		 sh 'chmod 777 /simulador'
                        try {
                            sh """
                                scp -o StrictHostKeyChecking=no -r * ${SERVER_USER}@${SERVER_HOST}:${DEPLOY_PATH}
                            """
                        } catch (Exception e) {
                            error("Deploy falhou.")
                        }
                    }
                }
            }
        }

        stage('Configurar Cron') {
            steps {
                script {
                    if (fileExists('config_cron.sh')) {
                        sh 'chmod +x config_cron.sh'
                        sh './config_cron.sh'
                    } else {
                        echo 'Arquivo config_cron.sh não encontrado.'
                    }
                }
            }
        }
    }

    post {
        success {
            echo 'Pipeline concluído com sucesso!'
        }
        failure {
            echo 'Falha na execução do pipeline.'
        }
        always {
            deleteDir()
        }
    }
}