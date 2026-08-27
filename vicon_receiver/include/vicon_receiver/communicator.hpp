#if !defined(COMMUNICATOR_HPP)
#define COMMUNICATOR_HPP

#include "boost/thread.hpp"
#include "publisher.hpp"
#include "rclcpp/rclcpp.hpp"
#include "vicon-datastream-sdk/DataStreamClient.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unistd.h>

#include "geometry_msgs/msg/pose_stamped.hpp"
#include "geometry_msgs/msg/transform_stamped.hpp"
#include "diagnostic_msgs/msg/diagnostic_status.hpp"
#include "nav_msgs/msg/odometry.hpp"

#include "tf2/LinearMath/Quaternion.h"
#include "tf2/LinearMath/Transform.h"

#include "tf2_geometry_msgs/tf2_geometry_msgs.hpp"
#include "tf2_ros/static_transform_broadcaster.h"
#include "tf2_ros/transform_broadcaster.h"

using namespace std;

// Main Node class
class Communicator : public rclcpp::Node
{
private:
  ViconDataStreamSDK::CPP::Client vicon_client;

  string hostname;
  unsigned int buffer_size;
  string ns_name;
  map<string, Publisher> pub_map;
  boost::mutex mutex;
  std::set<std::string> pending_publishers;

  geometry_msgs::msg::TransformStamped static_tf;
  string world_frame;
  string vicon_frame;
  vector<double> map_xyz;
  vector<double> map_rpy;
  bool map_rpy_in_degrees;

  // Name of the tracked body whose pose at startup becomes the world origin.
  string origin_subject;

  // Stream health, measured from vicon's own frame counter rather than wall-clock arrival times.
  double expected_rate_hz;
  double rate_warn_fraction;
  double rate_clear_fraction;
  double rate_window_seconds;

  unsigned int last_frame_number_ = 0;
  rclcpp::Time window_start_;
  unsigned int window_frames_ = 0;    // frames we actually processed
  unsigned int window_advance_ = 0;   // frames vicon's counter advanced
  unsigned int window_worst_gap_ = 0; // largest single jump, in frames
  bool stream_degraded_ = false;

  // Blackout watchdog. get_frame() blocks in ClientPull, so a dead vicon PC stops the main loop
  // entirely and nothing there can report it. This runs on its own thread so it still fires.
  double blackout_timeout_seconds;
  std::atomic<int64_t> last_frame_ns_{0};   // 0 = no frame yet, so connect() cannot trip it
  std::atomic<bool> watchdog_running_{false};
  bool blackout_warned_ = false;
  boost::thread watchdog_thread_;

  rclcpp::Publisher<diagnostic_msgs::msg::DiagnosticStatus>::SharedPtr status_publisher_;
  double origin_min_distance;
  bool origin_latched_ = false;

  std::shared_ptr<tf2_ros::StaticTransformBroadcaster> tf_static_broadcaster_;
  std::shared_ptr<tf2_ros::TransformBroadcaster> tf_broadcaster_;

  void publish_static_transform();

  // Track delivered vs produced frames and warn when the stream degrades.
  void monitor_stream(unsigned int frame_number);

  // Runs on its own thread: warns when no frame has arrived for blackout_timeout_seconds.
  void watchdog_loop();

  // Make boot, a pose measured in the vicon frame, the origin of the world frame.
  void latch_origin(const geometry_msgs::msg::Transform & boot);

public:
  Communicator();
  ~Communicator();

  // Initialises the connection to the DataStream server
  bool connect();

  // Stops the current connection to a DataStream server (if any).
  bool disconnect();

  // Main loop that request frames from the currently connected DataStream server and send the
  // received segment data to the Publisher class.
  void get_frame();

  // functions to create a segment publisher in a new thread
  void create_publisher(const string subject_name, const string segment_name);
  void create_publisher_thread(const string subject_name, const string segment_name);
};

#endif // COMMUNICATOR_HPP
