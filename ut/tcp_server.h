#pragma once

#include <memory>

namespace timeplus {

class InputStream;
class OutputStream;

class LocalTcpServer {
public:
    LocalTcpServer(int port);
    ~LocalTcpServer();

    void start();
    void stop();

private:

    int port_;
    int serverSd_;
};

}
