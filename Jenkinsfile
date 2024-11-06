pipeline {
    agent any

    environment {
        REPO_URL = 'https://github.com/Project-Horus-G9/simulador-de-dados'
        DEPLOY_PATH = '/simulador'
        SERVER_USER = 'ubuntu'
        SERVER_HOST = '18.212.95.38'
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

        stage('Start Server') {
            steps {
                sshagent(['aws-ssh-key']) {
                    script {
                        try {
                            // Rodando o servidor Python (substitua pelo seu arquivo Python)
                            sh """
                                ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER_HOST} 'cd ${DEPLOY_PATH} && python3 app.py'
                            """
                        } catch (Exception e) {
                            error("Falha ao iniciar o servidor.")
                        }
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