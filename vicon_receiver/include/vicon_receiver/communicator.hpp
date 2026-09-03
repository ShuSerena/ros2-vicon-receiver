#if !defined(COMMUNICATOR_HPP)
#define COMMUNICATOR_HPP

#include "boost/thread.hpp"
#include "publisher.hpp"
#include "rclcpp/rclcpp.hpp"
#include "vicon-datastream-sdk/DataStreamClient.h"
#include <atomic>
#include <mutex>
#include <chrono>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unistd.h>

#include "geometry_msgs/msg/pose_stamped.hpp"
#include "geometry_msgs/msg/twist_stamped.hpp"
#include "geometry_msgs/msg/transform_stamped.hpp"
#include "diagnostic_msgs/msg/diagnostic_status.hpp"
#include "nav_msgs/msg/odometry.hpp"

#include "tf2/LinearMath/Quaternion.h"
#include "tf2/LinearMath/Transform.h"
#include "tf2/LinearMath/Vector3.h"

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
  double expected_rate_hz = 200.0;
  double rate_warn_fraction = 0.8;
  double rate_clear_fraction = 0.9;
  double rate_window_seconds = 1.0;

  unsigned int last_frame_number_ = 0;
  rclcpp::Time window_start_;
  unsigned int window_frames_ = 0;    // frames we actually processed
  unsigned int window_advance_ = 0;   // frames vicon's counter advanced
  unsigned int window_worst_gap_ = 0; // largest single jump, in frames
  bool stream_degraded_ = false;

  // Blackout watchdog. get_frame() blocks in ClientPull, so a dead vicon PC stops the main loop
  // entirely and nothing there can report it. This runs on its own thread so it still fires.
  double blackout_timeout_seconds = 0.02;
  std::atomic<int64_t> last_frame_ns_{0};   // 0 = no frame yet, so connect() cannot trip it
  std::atomic<bool> watchdog_running_{false};
  bool blackout_active_ = false;         // watchdog thread only
  double blackout_worst_silent_ = 0.0;   // longest silence in the current blackout, for recovery
  boost::thread watchdog_thread_;

  rclcpp::Publisher<diagnostic_msgs::msg::DiagnosticStatus>::SharedPtr status_publisher_;
  double origin_min_distance = 0.01;
  bool origin_latched_ = false;

  // Vicon reports pose only - no SDK call carries velocity - so the twist is differenced here
  // from successive frames, per segment.
  struct SegmentMotion
  {
    bool valid = false;                   // a previous pose is stored to difference against
    bool filtered = false;                // the low-pass holds an estimate, not its zero init
    unsigned int frame = 0;               // vicon frame the stored pose came from
    tf2::Vector3 position;
    tf2::Quaternion orientation;
    tf2::Vector3 linear{0.0, 0.0, 0.0};   // world frame
    tf2::Vector3 angular{0.0, 0.0, 0.0};  // body frame
  };
  map<string, SegmentMotion> motion_map; // touched only from the main loop, so unguarded

  // Where the odom twist comes from. "computed" differences successive poses here; "vrpn"
  // copies what Tracker's own VRPN output reports, relayed by the vrpn_mocap node. Both are
  // finite differences of the same pose - the point of the switch is to compare them on
  // identical input, especially across occlusions and marker swaps.
  string twist_source;
  string vrpn_namespace;

  // One compact line per published odom, throttled. 0 disables it. Debug aid only: at frame rate
  // an unthrottled print is both unreadable and slow enough to make the loop miss frames.
  int odom_log_interval_ms = 0;

  struct VrpnTwist
  {
    rclcpp::Subscription<geometry_msgs::msg::TwistStamped>::SharedPtr sub;
    geometry_msgs::msg::Twist latest;
    rclcpp::Time stamp{0, 0, RCL_ROS_TIME};
    bool received = false;
  };
  map<string, VrpnTwist> vrpn_map; // keyed by subject name
  // The subscription callbacks run on the executor thread, the lookups on the vicon
  // loop, so every touch of vrpn_map is guarded.
  std::mutex vrpn_mutex_;

  // Vicon's own rate, read from the server once a frame has arrived; expected_rate_hz until then.
  double frame_rate_hz = 200.0;
  bool frame_rate_known_ = false;
  double velocity_cutoff_hz = 25.0;
  double angular_velocity_cutoff_hz = 12.0;
  int velocity_max_gap_frames = 10;

  std::shared_ptr<tf2_ros::StaticTransformBroadcaster> tf_static_broadcaster_;
  std::shared_ptr<tf2_ros::TransformBroadcaster> tf_broadcaster_;

  void publish_static_transform();

  // Track delivered vs produced frames and warn when the stream degrades.
  void monitor_stream(unsigned int frame_number);

  // Runs on its own thread: warns when no frame has arrived for blackout_timeout_seconds.
  void watchdog_loop();

  // Make boot, a pose measured in the vicon frame, the origin of the world frame.
  void latch_origin(const geometry_msgs::msg::Transform & boot);

  // Difference this segment's pose against the previous frame's to fill odom's twist.
  void fill_twist(
    const string & key, unsigned int frame_number, const geometry_msgs::msg::Pose & world_pose,
    nav_msgs::msg::Odometry & odom);

  // Fill odom's twist from the newest vrpn_mocap message for this subject instead.
  void fill_twist_from_vrpn(const string & subject, nav_msgs::msg::Odometry & odom);

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
