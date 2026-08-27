#include "vicon_receiver/communicator.hpp"

#include <cmath>

using namespace ViconDataStreamSDK::CPP;

// Constructor for the Communicator class
Communicator::Communicator() : Node("vicon_client")
{
  // Declare parameters for hostname, buffer size, and namespace
  this->declare_parameter<std::string>("hostname", "127.0.0.1");
  this->declare_parameter<int>("buffer_size", 200);
  this->declare_parameter<std::string>("namespace", "vicon");
  this->declare_parameter<std::string>("world_frame", "map");
  this->declare_parameter<std::string>("vicon_frame", "vicon");
  this->declare_parameter<std::vector<double>>("map_xyz", {0.0, 0.0, 0.0});
  this->declare_parameter<std::vector<double>>("map_rpy", {0.0, 0.0, 0.0});
  this->declare_parameter<bool>("map_rpy_in_degrees", false);
  this->declare_parameter<std::string>("origin_subject", "");
  this->declare_parameter<double>("origin_min_distance", 0.01);
  this->declare_parameter<double>("expected_rate_hz", 200.0);
  this->declare_parameter<double>("rate_warn_fraction", 0.8);
  this->declare_parameter<double>("rate_clear_fraction", 0.9);
  this->declare_parameter<double>("rate_window_seconds", 1.0);
  this->declare_parameter<double>("blackout_timeout_seconds", 0.5);

  // Retrieve parameters values
  this->get_parameter("hostname", hostname);
  this->get_parameter("buffer_size", buffer_size);
  this->get_parameter("namespace", ns_name);

  this->get_parameter("world_frame", world_frame);
  this->get_parameter("vicon_frame", vicon_frame);
  this->get_parameter("map_xyz", map_xyz);
  this->get_parameter("map_rpy", map_rpy);
  this->get_parameter("map_rpy_in_degrees", map_rpy_in_degrees);
  this->get_parameter("origin_subject", origin_subject);
  this->get_parameter("origin_min_distance", origin_min_distance);
  this->get_parameter("expected_rate_hz", expected_rate_hz);
  this->get_parameter("rate_warn_fraction", rate_warn_fraction);
  this->get_parameter("rate_clear_fraction", rate_clear_fraction);
  this->get_parameter("rate_window_seconds", rate_window_seconds);
  this->get_parameter("blackout_timeout_seconds", blackout_timeout_seconds);

  // Required: without it the node would silently publish raw vicon coordinates, and a drone
  // flying against a frame it did not expect is worse than a node that refuses to start.
  if (origin_subject.empty())
  {
    RCLCPP_FATAL(this->get_logger(), "origin_subject must name the drone's vicon subject");
    throw std::runtime_error("missing origin_subject");
  }

  // Publish static transform from map to vicon origin
  if (map_rpy_in_degrees)
  {
    for (unsigned int i = 0; i < map_rpy.size(); i++)
    {
      map_rpy[i] = map_rpy[i] * M_PI / 180.0;
    }
  }
  tf_static_broadcaster_ = std::make_shared<tf2_ros::StaticTransformBroadcaster>(this);
  this->publish_static_transform();

  // Initialize the tf2 broadcaster
  tf_broadcaster_ = std::make_shared<tf2_ros::TransformBroadcaster>(this);

  status_publisher_ = this->rclcpp::Node::create_publisher<diagnostic_msgs::msg::DiagnosticStatus>(
    "~/stream_status", 10);
  window_start_ = this->get_clock()->now();

  watchdog_running_ = true;
  watchdog_thread_ = boost::thread(&Communicator::watchdog_loop, this);

  RCLCPP_INFO(
    this->get_logger(), "Waiting for '%s' to set the world origin", origin_subject.c_str());
}

Communicator::~Communicator()
{
  watchdog_running_ = false;
  if (watchdog_thread_.joinable())
  {
    watchdog_thread_.join();
  }
}

// Independent of the vicon loop on purpose: in ClientPull GetFrame() blocks, so if the vicon PC
// stops answering, get_frame() never returns and nothing inside it can raise the alarm.
void Communicator::watchdog_loop()
{
  while (watchdog_running_ && rclcpp::ok())
  {
    boost::this_thread::sleep_for(boost::chrono::milliseconds(50));

    const int64_t last = last_frame_ns_.load();
    if (last == 0)
    {
      continue;  // nothing received yet; connecting is not a blackout
    }

    const double silent = (this->get_clock()->now().nanoseconds() - last) / 1e9;

    if (!blackout_warned_ && silent > blackout_timeout_seconds)
    {
      blackout_warned_ = true;
      RCLCPP_ERROR(
        this->get_logger(), "Vicon blackout: no frame for %.2f s (expected one every %.1f ms)",
        silent, 1000.0 / expected_rate_hz);

      diagnostic_msgs::msg::DiagnosticStatus status;
      status.name = "vicon_stream";
      status.hardware_id = hostname;
      status.level = diagnostic_msgs::msg::DiagnosticStatus::ERROR;
      status.message = "blackout";
      diagnostic_msgs::msg::KeyValue kv;
      kv.key = "silent_seconds";
      kv.value = std::to_string(silent);
      status.values.push_back(kv);
      status_publisher_->publish(status);
    }
    else if (blackout_warned_ && silent < blackout_timeout_seconds)
    {
      blackout_warned_ = false;
      RCLCPP_INFO(this->get_logger(), "Vicon frames resumed after %.2f s", silent);
    }
  }
}

// Publish the static transform from map to vicon origin
void Communicator::publish_static_transform()
{
  static_tf.header.stamp = this->get_clock()->now();
  static_tf.header.frame_id = world_frame;
  static_tf.child_frame_id = vicon_frame;

  static_tf.transform.translation.x = map_xyz[0];
  static_tf.transform.translation.y = map_xyz[1];
  static_tf.transform.translation.z = map_xyz[2];
  tf2::Quaternion q;
  q.setRPY(
    map_rpy[0],
    map_rpy[1],
    map_rpy[2]);
  static_tf.transform.rotation.x = q.x();
  static_tf.transform.rotation.y = q.y();
  static_tf.transform.rotation.z = q.z();
  static_tf.transform.rotation.w = q.w();

  tf_static_broadcaster_->sendTransform(static_tf);

  string msg = "Published static transform from " + world_frame + " to " + vicon_frame;
  cout << msg << endl;
}

// Make boot, a pose measured in the vicon frame, the origin of the world frame.
//
// Every published pose is run through the static world_frame -> vicon_frame transform, so setting
// that transform to boot's inverse re-expresses all of them relative to boot:
//   p_world = R_boot^T (p_vicon - p_boot),   q_world = q_boot^-1 * q_vicon
// Boot itself therefore comes out as (0,0,0) / (0,0,0,1).
void Communicator::latch_origin(const geometry_msgs::msg::Transform & boot)
{
  const tf2::Quaternion q(boot.rotation.x, boot.rotation.y, boot.rotation.z, boot.rotation.w);
  const tf2::Vector3 p(boot.translation.x, boot.translation.y, boot.translation.z);

  // Vicon reports a body it is not tracking as exactly (0,0,0) with a meaningless rotation, and
  // does not always set the Occluded flag for it. Latching that would pin the world frame to the
  // vicon origin, and a zero rotation would invert to NaN, neither of which ever recovers.
  const double qn = q.length2();
  if (p.length2() < origin_min_distance * origin_min_distance || !std::isfinite(qn) ||
      std::abs(qn - 1.0) > 1e-3)
  {
    RCLCPP_WARN_THROTTLE(
      this->get_logger(), *this->get_clock(), 2000,
      "'%s' is not tracked yet: xyz [%.4f %.4f %.4f], |q|^2 %.4f - not latching",
      origin_subject.c_str(), p.x(), p.y(), p.z(), qn);
    return;
  }

  const tf2::Transform vicon_T_boot(q, p);

  static_tf.header.stamp = this->get_clock()->now();
  static_tf.header.frame_id = world_frame;
  static_tf.child_frame_id = vicon_frame;
  static_tf.transform = tf2::toMsg(vicon_T_boot.inverse());

  tf_static_broadcaster_->sendTransform(static_tf);
  origin_latched_ = true;

  RCLCPP_INFO(
    this->get_logger(), "World origin set from '%s' at vicon xyz [%.4f %.4f %.4f]",
    origin_subject.c_str(), boot.translation.x, boot.translation.y, boot.translation.z);
}

// Connect to the Vicon server
bool Communicator::connect()
{
  // Log connection attempt
  string msg = "Connecting to " + hostname + " ...";
  cout << msg << endl;

  int counter = 0;
  // Retry connection until successful
  while (!vicon_client.IsConnected().Connected && rclcpp::ok())
  {
    bool ok = (vicon_client.Connect(hostname).Result == Result::Success);
    if (!ok)
    {
      counter++;
      msg = "Connect failed, reconnecting (" + std::to_string(counter) + ")...";
      cout << msg << endl;
    }
  }
  if (!rclcpp::ok())
  {
    std::cout << "Shutdown requested before connection established." << std::endl;
    return false;
  }

  // Log successful connection
  msg = "Connection successfully established with " + hostname;
  cout << msg << endl;

  // Enable various data streams from the Vicon server
  vicon_client.EnableSegmentData();
  vicon_client.EnableMarkerData();
  vicon_client.EnableUnlabeledMarkerData();
  vicon_client.EnableMarkerRayData();
  vicon_client.EnableDeviceData();
  vicon_client.EnableDebugData();

  // Set the stream mode and buffer size
  vicon_client.SetStreamMode(StreamMode::ClientPull);
  vicon_client.SetBufferSize(buffer_size);

  // Log initialization completion
  msg = "Initialization complete";
  cout << msg << endl;

  return true;
}

// Disconnect from the Vicon server
bool Communicator::disconnect()
{
  // If already disconnected, return true
  if (!vicon_client.IsConnected().Connected)
    return true;

  sleep(1); // Wait before disconnecting

  // Disable all data streams
  vicon_client.DisableSegmentData();
  vicon_client.DisableMarkerData();
  vicon_client.DisableUnlabeledMarkerData();
  vicon_client.DisableDeviceData();
  vicon_client.DisableCentroidData();

  // Log disconnection attempt
  string msg = "Disconnecting from " + hostname + "...";
  cout << msg << endl;

  // Disconnect from the server
  vicon_client.Disconnect();

  // Log successful disconnection
  msg = "Successfully disconnected";
  cout << msg << endl;

  // Verify disconnection
  return !vicon_client.IsConnected().Connected;
}

// Vicon's frame counter advances at the system rate no matter what this node does, so comparing
// frames processed against counter advance separates "vicon did not send it" from "we did not
// read it in time" - in ClientPull the client sets the pace, so a slow loop silently skips frames.
void Communicator::monitor_stream(unsigned int frame_number)
{
  if (last_frame_number_ == 0)
  {
    last_frame_number_ = frame_number;
    return;
  }

  const long advance = static_cast<long>(frame_number) - static_cast<long>(last_frame_number_);
  last_frame_number_ = frame_number;

  // Only a counter reset (tracker restart, or wrap) invalidates the window. A large positive
  // jump is a real dropout and must be reported, not swallowed.
  if (advance < 0)
  {
    window_frames_ = 0;
    window_advance_ = 0;
    window_worst_gap_ = 0;
    window_start_ = this->get_clock()->now();
    return;
  }

  // The same frame served twice is not new data: counting it would make a stalled stream, where
  // GetFrame keeps returning the last frame, look healthy. Skip the counters but still fall
  // through to the window check, or a total stall would never close a window and never warn.
  if (advance > 0)
  {
    window_frames_++;
    window_advance_ += static_cast<unsigned int>(advance);
    window_worst_gap_ = std::max(window_worst_gap_, static_cast<unsigned int>(advance));
  }

  const double elapsed = (this->get_clock()->now() - window_start_).seconds();
  if (elapsed < rate_window_seconds)
  {
    return;
  }

  // Fraction of the frames vicon should have produced in this window that reached a subscriber.
  const double delivered = window_frames_ / (elapsed * expected_rate_hz);
  // Fraction of the frames vicon actually produced that we managed to read.
  const double kept_up = window_advance_ > 0
    ? window_frames_ / static_cast<double>(window_advance_)
    : 0.0;
  const double worst_gap_ms = 1000.0 * window_worst_gap_ / expected_rate_hz;

  if (!stream_degraded_ && delivered < rate_warn_fraction)
  {
    stream_degraded_ = true;
    RCLCPP_WARN(
      this->get_logger(),
      "Vicon stream degraded: %.0f%% of %.0f Hz (%u frames in %.2f s); read %.0f%% of what vicon "
      "produced; worst gap %u frames (%.0f ms)",
      100.0 * delivered, expected_rate_hz, window_frames_, elapsed, 100.0 * kept_up,
      window_worst_gap_, worst_gap_ms);
  }
  else if (stream_degraded_ && delivered > rate_clear_fraction)
  {
    stream_degraded_ = false;
    RCLCPP_INFO(
      this->get_logger(), "Vicon stream recovered: %.0f%% of %.0f Hz", 100.0 * delivered,
      expected_rate_hz);
  }

  diagnostic_msgs::msg::DiagnosticStatus status;
  status.name = "vicon_stream";
  status.hardware_id = hostname;
  status.level = stream_degraded_ ? diagnostic_msgs::msg::DiagnosticStatus::WARN
                                  : diagnostic_msgs::msg::DiagnosticStatus::OK;
  status.message = stream_degraded_ ? "degraded" : "ok";
  auto kv = [&status](const string & k, double v) {
    diagnostic_msgs::msg::KeyValue p;
    p.key = k;
    p.value = std::to_string(v);
    status.values.push_back(p);
  };
  kv("delivered_fraction", delivered);
  kv("kept_up_fraction", kept_up);
  kv("worst_gap_ms", worst_gap_ms);
  kv("rate_hz", window_frames_ / elapsed);
  status_publisher_->publish(status);

  window_frames_ = 0;
  window_advance_ = 0;
  window_worst_gap_ = 0;
  window_start_ = this->get_clock()->now();
}

// Retrieve and process a frame of data from the Vicon server
void Communicator::get_frame()
{
  // Request a new frame
  vicon_client.GetFrame();
  Output_GetFrameNumber frame_number = vicon_client.GetFrameNumber();

  last_frame_ns_ = this->get_clock()->now().nanoseconds();
  monitor_stream(frame_number.FrameNumber);

  cout << "frame number: " << frame_number.FrameNumber << endl;

  // Get the number of subjects in the frame
  unsigned int subject_count = vicon_client.GetSubjectCount().SubjectCount;

  map<string, Publisher>::iterator pub_it;

  // Collected only while waiting, so a misspelled origin_subject names its alternatives.
  string seen_subjects;

  // Iterate through each subject
  for (unsigned int subject_index = 0; subject_index < subject_count; ++subject_index)
  {
    // Get the subject name
    string subject_name = vicon_client.GetSubjectName(subject_index).SubjectName;

    if (!origin_latched_)
    {
      seen_subjects += (seen_subjects.empty() ? "" : ", ") + subject_name;
    }

    // Get the number of segments for the subject
    unsigned int segment_count = vicon_client.GetSegmentCount(subject_name).SegmentCount;

    // Iterate through each segment
    for (unsigned int segment_index = 0; segment_index < segment_count; ++segment_index)
    {
      // Get the segment name
      string segment_name = vicon_client.GetSegmentName(subject_name, segment_index).SegmentName;

      // Retrieve the segment's global position and rotation
      Output_GetSegmentGlobalTranslation trans =
        vicon_client.GetSegmentGlobalTranslation(subject_name, segment_name);
      Output_GetSegmentGlobalRotationQuaternion quat =
        vicon_client.GetSegmentGlobalRotationQuaternion(subject_name, segment_name);

      // Build a TF message for this segment
      geometry_msgs::msg::TransformStamped tf_msg;

      // Use node clock to timestamp the transform
      tf_msg.header.stamp = this->get_clock()->now();

      // Parent and child frames: Vicon global origin -> subject_segment
      tf_msg.header.frame_id = vicon_frame;
      tf_msg.child_frame_id = subject_name + "_" + segment_name;

      // Vicon translations are in millimeters; convert to meters for ROS
      tf_msg.transform.translation.x = trans.Translation[0] / 1000.0;
      tf_msg.transform.translation.y = trans.Translation[1] / 1000.0;
      tf_msg.transform.translation.z = trans.Translation[2] / 1000.0;

      // Vicon quaternion order is [x, y, z, w]; copy directly
      tf_msg.transform.rotation.x = quat.Rotation[0];
      tf_msg.transform.rotation.y = quat.Rotation[1];
      tf_msg.transform.rotation.z = quat.Rotation[2];
      tf_msg.transform.rotation.w = quat.Rotation[3];

      cout << "Received segment data for " << subject_name << "/" << segment_name
           << ": translation = [" << tf_msg.transform.translation.x << ", "
           << tf_msg.transform.translation.y << ", "
           << tf_msg.transform.translation.z << "], rotation = ["
           << tf_msg.transform.rotation.x << ", "
           << tf_msg.transform.rotation.y << ", "
           << tf_msg.transform.rotation.z << ", "
           << tf_msg.transform.rotation.w << "]" << endl;

      // Vicon reports a body it is not tracking as exactly (0,0,0). Do NOT use the SDK's
      // Occluded flags for this: they read false for an untracked body here, so they cannot be
      // trusted in either direction, and gating on them silently drops good data. The sentinel
      // is the observable signal. Exact zero only, so a body legitimately passing near the
      // vicon origin is never censored.
      const bool tracked = !(tf_msg.transform.translation.x == 0.0 &&
                             tf_msg.transform.translation.y == 0.0 &&
                             tf_msg.transform.translation.z == 0.0);

      if (!tracked)
      {
        RCLCPP_WARN_THROTTLE(
          this->get_logger(), *this->get_clock(), 2000,
          "'%s/%s' reads (0,0,0): vicon is not tracking it, so nothing is published for it",
          subject_name.c_str(), segment_name.c_str());
      }

      // Set the world origin from the drone's first tracked pose.
      if (!origin_latched_ && tracked &&
          (origin_subject == subject_name ||
           origin_subject == subject_name + "/" + segment_name))
      {
        latch_origin(tf_msg.transform);
      }

      // Publish the position data
      boost::mutex::scoped_try_lock lock(mutex);
      if (lock.owns_lock())
      {
        // Check if a publisher exists for the segment
        pub_it = pub_map.find(subject_name + "/" + segment_name);
        if (pub_it != pub_map.end())
        {
          Publisher &pub = pub_it->second;

          if (pub.is_ready && origin_latched_ && tracked)
          {
            // Build a PoseStamped in the Vicon frame from the computed TransformStamped.
            geometry_msgs::msg::PoseStamped vicon_pose_msg;

            // Header: copy timestamp and frame_id ("vicon") from the transform header.
            vicon_pose_msg.header = tf_msg.header;

            // Position: copy the already meter-converted translation components.
            vicon_pose_msg.pose.position.x = tf_msg.transform.translation.x;
            vicon_pose_msg.pose.position.y = tf_msg.transform.translation.y;
            vicon_pose_msg.pose.position.z = tf_msg.transform.translation.z;

            // Orientation: copy the quaternion (x, y, z, w) directly from the transform.
            vicon_pose_msg.pose.orientation = tf_msg.transform.rotation;

            // Update timestamp of static transform
            static_tf.header.stamp = tf_msg.header.stamp;

            // Transform the pose to the global frame
            geometry_msgs::msg::PoseStamped global_pose_msg;
            tf2::doTransform(vicon_pose_msg, global_pose_msg, static_tf);

            // Wrap the transformed pose as odometry. Vicon measures pose only, so twist is
            // left at zero rather than filled with a differentiated guess.
            nav_msgs::msg::Odometry odom_msg;
            odom_msg.header = global_pose_msg.header;      // stamped in world_frame by doTransform
            odom_msg.child_frame_id = tf_msg.child_frame_id;
            odom_msg.pose.pose = global_pose_msg.pose;

            pub.publish(odom_msg);
            cout << "Published vicon_pose_msg: [" << vicon_pose_msg.pose.position.x << ", " << vicon_pose_msg.pose.position.y << ", " << vicon_pose_msg.pose.position.z << "]" << endl;
          }
        }
        else
        {
          cout << "No publisher found for segment " << segment_name << ", creating one..." << endl;
          // Create a publisher if it doesn't exist, de-duplicating concurrent attempts
          std::string key = subject_name + "/" + segment_name;
          if (pending_publishers.find(key) == pending_publishers.end())
          {
            pending_publishers.insert(key);
            lock.unlock();
            create_publisher(subject_name, segment_name);
          }
          else
          {
            // Another thread is already creating this publisher
            lock.unlock();
          }
        }
      }

      // Broadcast the transform
      tf_broadcaster_->sendTransform(tf_msg);
    }
  }

  if (!origin_latched_)
  {
    RCLCPP_WARN_THROTTLE(
      this->get_logger(), *this->get_clock(), 2000,
      "Waiting for a clean pose of '%s'; nothing published yet. Subjects seen: [%s]",
      origin_subject.c_str(), seen_subjects.c_str());
  }
}

// Create a publisher for a specific subject and segment
void Communicator::create_publisher(const string subject_name, const string segment_name)
{
  // Launch a thread to create the publisher
  boost::thread(&Communicator::create_publisher_thread, this, subject_name, segment_name);
}

// Thread function to create a publisher
void Communicator::create_publisher_thread(const string subject_name, const string segment_name)
{
  // Construct the topic name and key
  std::string topic_name = ns_name + "/" + subject_name + "/" + segment_name;
  std::string key = subject_name + "/" + segment_name;

  // Log publisher creation
  string msg = "Creating publisher for segment " + segment_name + " from subject " + subject_name;
  cout << msg << endl;

  // Create and store the publisher; then clear the pending flag
  boost::mutex::scoped_lock lock(mutex);
  pub_map.insert(std::map<std::string, Publisher>::value_type(key, Publisher(topic_name, this)));
  pending_publishers.erase(key);
  lock.unlock();
}

// Main function
int main(int argc, char **argv)
{
  // Initialize the ROS 2 node
  rclcpp::init(argc, argv);
  auto node = std::make_shared<Communicator>();

  // Connect to the Vicon server
  node->connect();

  // Continuously retrieve frames while ROS 2 is running
  while (rclcpp::ok())
  {
    node->get_frame();
  }

  // Disconnect from the Vicon server and shut down ROS 2
  node->disconnect();
  rclcpp::shutdown();
  return 0;
}