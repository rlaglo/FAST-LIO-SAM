#include "fast_lio_sam.h"
#include <csignal>
#include <thread>

static volatile std::sig_atomic_t g_shutdown_requested = 0;

void signalHandler(int signum)
{
    if (signum == SIGINT || signum == SIGTERM)
    {
        g_shutdown_requested = 1;
    }
}

int main(int argc, char **argv)
{
    ros::init(argc, argv, "fast_lio_sam_node", ros::init_options::NoSigintHandler);
    ros::NodeHandle nh_private("~");
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    FastLioSam fast_lio_sam_(nh_private);

    ros::AsyncSpinner spinner(4); // Use multi threads
    spinner.start();

    while (ros::ok() && !g_shutdown_requested)
    {
        if (fast_lio_sam_.shouldShutdownForIdle())
        {
            g_shutdown_requested = 1;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    spinner.stop();

    // Give callbacks a short moment to complete before object destruction.
    ros::Duration(0.5).sleep();

    fast_lio_sam_.shutdownAndSave();

    if (ros::ok())
    {
        ros::shutdown();
    }

    return 0;  // Automatic destructor call
}
