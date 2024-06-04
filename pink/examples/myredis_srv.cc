#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <map>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"

int rondb_connect(const char *conn_string, unsigned num_connections);
void rondb_end();
int rondb_redis_handler(pink::RedisCmdArgsType& argv,
                        std::string* response,
                        int fd);

using namespace pink;

std::map<std::string, std::string> db;


class MyConn: public RedisConn {
 public:
  MyConn(int fd, const std::string& ip_port, ServerThread *thread,
         void* worker_specific_data);
  virtual ~MyConn() = default;

 protected:
  int DealMessage(RedisCmdArgsType& argv, std::string* response) override;

 private:
};

MyConn::MyConn(int fd, const std::string& ip_port,
               ServerThread *thread, void* worker_specific_data)
    : RedisConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

int MyConn::DealMessage(RedisCmdArgsType& argv, std::string* response) {
  printf("Get redis message ");
  for (int i = 0; i < argv.size(); i++) {
    printf("%s ", argv[i].c_str());
  }
  printf("\n");
  return rondb_redis_handler(argv, response, 0);

  std::string val = "result";
  std::string res;
  // set command
  if (argv.size() == 3) {
    response->append("+OK\r\n");
    db[argv[1]] = argv[2];
  } else if (argv.size() == 2) {
    std::map<std::string, std::string>::iterator iter = db.find(argv[1]);
    if (iter != db.end()) {
      const std::string& val = iter->second;
      response->append("*1\r\n$");
      response->append(std::to_string(val.length()));
      response->append("\r\n");
      response->append(val);
      response->append("\r\n");
    } else {
      response->append("$-1\r\n");
    }
  } else {
    response->append("+OK\r\n");
  }
  return 0;
}

class MyConnFactory : public ConnFactory {
 public:
  virtual PinkConn *NewPinkConn(int connfd, const std::string &ip_port,
                                ServerThread *thread,
                                void* worker_specific_data) const {
    return new MyConn(connfd, ip_port, thread, worker_specific_data);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  running.store(false);
  printf("server Exit");
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char* argv[]) {
  SignalSetup();

  printf("Connecting to RonDB\n");
  rondb_connect("localhost:13060", 1);
  printf("Connected to RonDB\n");

  if (argc < 2) {
    printf("server will listen to 6379\n");
  } else {
    printf("server will listen to %d\n", atoi(argv[1]));
  }
  int my_port = (argc > 1) ? atoi(argv[1]) : 6379;

  printf("Create MyConnFactory\n");
  ConnFactory *conn_factory = new MyConnFactory();

  printf("Create HolyThread\n");
  ServerThread* my_thread = NewHolyThread(my_port, conn_factory, 1000);
  printf("Start HolyThread\n");
  if (my_thread->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  printf("Stop HolyThread\n");
  my_thread->StopThread();
  printf("rondb_end\n");
  rondb_end();

  delete my_thread;
  delete conn_factory;

  return 0;
}
