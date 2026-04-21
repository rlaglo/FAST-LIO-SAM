#include "fast_lio_sam.h"

FastLioSam::FastLioSam(const ros::NodeHandle &n_private):
    nh_(n_private)
{
    ////// ROS params
    double loop_update_hz, vis_hz;
    LoopClosureConfig lc_config;
    /* basic */
    nh_.param<std::string>("/basic/map_frame", map_frame_, "map");
    nh_.param<double>("/basic/loop_update_hz", loop_update_hz, 1.0);
    nh_.param<double>("/basic/vis_hz", vis_hz, 0.5);
    /* keyframe */
    nh_.param<double>("/keyframe/keyframe_threshold", keyframe_thr_, 1.0);
    nh_.param<int>("/keyframe/num_submap_keyframes", lc_config.num_submap_keyframes_, 5);
    // Backward compatibility for older config typo key.
    nh_.param<int>("/keyframe/nusubmap_keyframes", lc_config.num_submap_keyframes_, lc_config.num_submap_keyframes_);
    /* loop */
    nh_.param<double>("/loop/loop_detection_radius", lc_config.loop_detection_radius_, 15.0);
    nh_.param<double>("/loop/loop_detection_timediff_threshold", lc_config.loop_detection_timediff_threshold_, 10.0);
    lc_config.icp_max_corr_dist_ = lc_config.loop_detection_radius_ * 1.5;
    /* icp */
    nh_.param<double>("/icp/icp_voxel_resolution", lc_config.voxel_res_, 0.3);
    nh_.param<double>("/icp/icp_score_threshold", lc_config.icp_score_threshold_, 0.3);
    /* results */
    nh_.param<double>("/result/save_voxel_resolution", voxel_res_, 0.3);
    nh_.param<bool>("/result/save_map_pcd", save_map_pcd_, false);
    nh_.param<bool>("/result/save_map_bag", save_map_bag_, false);
    nh_.param<bool>("/result/save_in_kitti_format", save_in_kitti_format_, false);
    nh_.param<int>("/result/save_max_points", save_max_points_, 9000000);
    nh_.param<bool>("/result/auto_save_on_idle", auto_save_on_idle_, true);
    nh_.param<double>("/result/bag_end_timeout_sec", bag_end_timeout_sec_, 30.0);
    nh_.param<std::string>("/result/seq_name", seq_name_, "");
    /* Loop closure */
    loop_closure_.reset(new LoopClosure(lc_config));
    /* Initialization of GTSAM */
    gtsam::ISAM2Params isam_params_;
    isam_params_.relinearizeThreshold = 0.01;
    isam_params_.relinearizeSkip = 1;
    isam_handler_ = std::make_shared<gtsam::ISAM2>(isam_params_);
    /* ROS things */
    odom_path_.header.frame_id = map_frame_;
    corrected_path_.header.frame_id = map_frame_;
    package_path_ = ros::package::getPath("fast_lio_sam");
    if (package_path_.empty())
    {
        package_path_ = fs::current_path().string();
        ROS_WARN("Package path lookup failed. Falling back to current path: %s", package_path_.c_str());
    }
    fs::create_directories(package_path_ + "/result");
    nh_.param<bool>("/result/save_mapped_txt", save_mapped_txt_, true);
    nh_.param<std::string>("/result/mapped_txt_path", mapped_txt_path_, std::string(""));
    nh_.param<bool>("/result/match_mapped_txt_baseline_length", match_mapped_txt_baseline_length_, false);
    nh_.param<std::string>("/result/mapped_txt_baseline_path", mapped_txt_baseline_path_, std::string(""));
    if (mapped_txt_path_.empty())
    {
        mapped_txt_path_ = package_path_ + "/result/mapped.txt";
    }
    if (save_mapped_txt_)
    {
        fs::path mapped_txt_parent = fs::path(mapped_txt_path_).parent_path();
        if (!mapped_txt_parent.empty())
        {
            fs::create_directories(mapped_txt_parent);
        }
        std::ofstream mapped_txt_file(mapped_txt_path_, std::ios::out);
        mapped_txt_file.close();
        ROS_INFO("FAST-LIO-SAM TUM path will be saved to %s", mapped_txt_path_.c_str());
    }
    /* publishers */
    odom_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/ori_odom", 10, true);
    path_pub_ = nh_.advertise<nav_msgs::Path>("/ori_path", 10, true);
    corrected_odom_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/corrected_odom", 10, true);
    corrected_path_pub_ = nh_.advertise<nav_msgs::Path>("/corrected_path", 10, true);
    corrected_pcd_map_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/corrected_map", 10, true);
    corrected_current_pcd_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/corrected_current_pcd", 10, true);
    loop_detection_pub_ = nh_.advertise<visualization_msgs::Marker>("/loop_detection", 10, true);
    realtime_pose_pub_ = nh_.advertise<geometry_msgs::PoseStamped>("/pose_stamped", 10);
    debug_src_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/src", 10, true);
    debug_dst_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/dst", 10, true);
    debug_fine_aligned_pub_ = nh_.advertise<sensor_msgs::PointCloud2>("/aligned", 10, true);
    /* subscribers */
    // 런치파일에서 /aft_mapped_to_init 로 리매핑 됐음
    sub_odom_ = std::make_shared<message_filters::Subscriber<nav_msgs::Odometry>>(nh_, "/Odometry", 10); 
    sub_pcd_ = std::make_shared<message_filters::Subscriber<sensor_msgs::PointCloud2>>(nh_, "/cloud_registered", 10);
    sub_odom_pcd_sync_ = std::make_shared<message_filters::Synchronizer<odom_pcd_sync_pol>>(odom_pcd_sync_pol(10), *sub_odom_, *sub_pcd_);
    sub_odom_pcd_sync_->registerCallback(boost::bind(&FastLioSam::odomPcdCallback, this, _1, _2));
    sub_save_flag_ = nh_.subscribe("/save_dir", 1, &FastLioSam::saveFlagCallback, this);
    /* Timers */
    loop_timer_ = nh_.createTimer(ros::Duration(1 / loop_update_hz), &FastLioSam::loopTimerFunc, this);
    vis_timer_ = nh_.createTimer(ros::Duration(1 / vis_hz), &FastLioSam::visTimerFunc, this);
    ROS_INFO("Main class, starting node...");
}

void FastLioSam::odomPcdCallback(const nav_msgs::OdometryConstPtr &odom_msg,
                                 const sensor_msgs::PointCloud2ConstPtr &pcd_msg)
{
    Eigen::Matrix4d last_odom_tf;
    last_odom_tf = current_frame_.pose_eig_;                              // to calculate delta
    current_frame_ = PosePcd(*odom_msg, *pcd_msg, current_keyframe_idx_); // to be checked if keyframe or not
    {
        std::lock_guard<std::mutex> idle_lock(idle_mutex_);
        last_input_time_ = ros::Time::now();
        idle_input_started_ = true;
    }
    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    {
        //// 1. realtime pose = last corrected odom * delta (last -> current)
        std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
        odom_delta_ = odom_delta_ * last_odom_tf.inverse() * current_frame_.pose_eig_;
        current_frame_.pose_corrected_eig_ = last_corrected_pose_ * odom_delta_;
        realtime_pose_pub_.publish(poseEigToPoseStamped(current_frame_.pose_corrected_eig_, map_frame_));
        broadcaster_.sendTransform(tf::StampedTransform(poseEigToROSTf(current_frame_.pose_corrected_eig_),
                                                        ros::Time::now(),
                                                        map_frame_,
                                                        "robot"));
    }
    corrected_current_pcd_pub_.publish(pclToPclRos(transformPcd(current_frame_.pcd_, current_frame_.pose_corrected_eig_), map_frame_));

    if (!is_initialized_) //// init only once
    {
        // others
        keyframes_.push_back(current_frame_);
        updateOdomsAndPaths(current_frame_);
        // graph
        auto variance_vector = (gtsam::Vector(6) << 1e-4, 1e-4, 1e-4, 1e-2, 1e-2, 1e-2).finished(); // rad*rad,
                                                                                                    // meter*meter
        gtsam::noiseModel::Diagonal::shared_ptr prior_noise = gtsam::noiseModel::Diagonal::Variances(variance_vector);
        gtsam_graph_.add(gtsam::PriorFactor<gtsam::Pose3>(0, poseEigToGtsamPose(current_frame_.pose_eig_), prior_noise));
        init_esti_.insert(current_keyframe_idx_, poseEigToGtsamPose(current_frame_.pose_eig_));
        current_keyframe_idx_++;
        is_initialized_ = true;
    }
    else
    {
        //// 2. check if keyframe
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        if (checkIfKeyframe(current_frame_, keyframes_.back()))
        {
            // 2-2. if so, save
            {
                std::lock_guard<std::mutex> lock(keyframes_mutex_);
                keyframes_.push_back(current_frame_);
            }
            // 2-3. if so, add to graph
            auto variance_vector = (gtsam::Vector(6) << 1e-4, 1e-4, 1e-4, 1e-2, 1e-2, 1e-2).finished();
            gtsam::noiseModel::Diagonal::shared_ptr odom_noise = gtsam::noiseModel::Diagonal::Variances(variance_vector);
            gtsam::Pose3 pose_from = poseEigToGtsamPose(keyframes_[current_keyframe_idx_ - 1].pose_corrected_eig_);
            gtsam::Pose3 pose_to = poseEigToGtsamPose(current_frame_.pose_corrected_eig_);
            {
                std::lock_guard<std::mutex> lock(graph_mutex_);
                gtsam_graph_.add(gtsam::BetweenFactor<gtsam::Pose3>(current_keyframe_idx_ - 1,
                                                                    current_keyframe_idx_,
                                                                    pose_from.between(pose_to),
                                                                    odom_noise));
                init_esti_.insert(current_keyframe_idx_, pose_to);
            }
            current_keyframe_idx_++;

            //// 3. vis
            high_resolution_clock::time_point t3 = high_resolution_clock::now();
            {
                std::lock_guard<std::mutex> lock(vis_mutex_);
                updateOdomsAndPaths(current_frame_);
            }

            //// 4. optimize with graph
            high_resolution_clock::time_point t4 = high_resolution_clock::now();
            // m_corrected_esti = gtsam::LevenbergMarquardtOptimizer(m_gtsam_graph, init_esti_).optimize(); // cf. isam.update vs values.LM.optimize
            {
                std::lock_guard<std::mutex> lock(graph_mutex_);
                isam_handler_->update(gtsam_graph_, init_esti_);
                isam_handler_->update();
                if (loop_added_flag_) // https://github.com/TixiaoShan/LIO-SAM/issues/5#issuecomment-653752936
                {
                    isam_handler_->update();
                    isam_handler_->update();
                    isam_handler_->update();
                }
                gtsam_graph_.resize(0);
                init_esti_.clear();
            }

            //// 5. handle corrected results
            // get corrected poses and reset odom delta (for realtime pose pub)
            high_resolution_clock::time_point t5 = high_resolution_clock::now();
            {
                std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
                corrected_esti_ = isam_handler_->calculateEstimate();
                last_corrected_pose_ = gtsamPoseToPoseEig(corrected_esti_.at<gtsam::Pose3>(corrected_esti_.size() - 1));
                odom_delta_ = Eigen::Matrix4d::Identity();
            }
            // correct poses in keyframes
            if (loop_added_flag_)
            {
                std::lock_guard<std::mutex> lock(keyframes_mutex_);
                for (size_t i = 0; i < corrected_esti_.size(); ++i)
                {
                    keyframes_[i].pose_corrected_eig_ = gtsamPoseToPoseEig(corrected_esti_.at<gtsam::Pose3>(i));
                }
                loop_added_flag_ = false;
            }
            high_resolution_clock::time_point t6 = high_resolution_clock::now();

            ROS_INFO("real: %.1f, key_add: %.1f, vis: %.1f, opt: %.1f, res: %.1f, tot: %.1fms",
                     duration_cast<microseconds>(t2 - t1).count() / 1e3,
                     duration_cast<microseconds>(t3 - t2).count() / 1e3,
                     duration_cast<microseconds>(t4 - t3).count() / 1e3,
                     duration_cast<microseconds>(t5 - t4).count() / 1e3,
                     duration_cast<microseconds>(t6 - t5).count() / 1e3,
                     duration_cast<microseconds>(t6 - t1).count() / 1e3);
        }
    }
    return;
}

bool FastLioSam::shouldShutdownForIdle()
{
    if (!auto_save_on_idle_)
    {
        return false;
    }

    std::lock_guard<std::mutex> idle_lock(idle_mutex_);
    if (!idle_input_started_ || idle_shutdown_triggered_)
    {
        return false;
    }

    const double idle_sec = (ros::Time::now() - last_input_time_).toSec();
    if (idle_sec < bag_end_timeout_sec_)
    {
        return false;
    }

    idle_shutdown_triggered_ = true;
    ROS_WARN("No synchronized odom/pcd input for %.1f sec. Triggering shutdown save.", idle_sec);
    return true;
}

void FastLioSam::loopTimerFunc(const ros::TimerEvent &event)
{
    (void)event;
    if (!is_initialized_ || keyframes_.empty())
    {
        return;
    }
    auto &latest_keyframe = keyframes_.back();
    if (latest_keyframe.processed_)
    {
        return;
    }
    latest_keyframe.processed_ = true;

    high_resolution_clock::time_point t1 = high_resolution_clock::now();
    const int closest_keyframe_idx = loop_closure_->fetchClosestKeyframeIdx(latest_keyframe, keyframes_);
    if (closest_keyframe_idx < 0)
    {
        return;
    }

    const RegistrationOutput &reg_output = loop_closure_->performLoopClosure(latest_keyframe, keyframes_, closest_keyframe_idx);
    if (reg_output.is_valid_)
    {
        ROS_INFO("\033[1;32mLoop closure accepted. Score: %.3f\033[0m", reg_output.score_);
        const auto &score = reg_output.score_;
        gtsam::Pose3 pose_from = poseEigToGtsamPose(reg_output.pose_between_eig_ * latest_keyframe.pose_corrected_eig_); // IMPORTANT: take care of the order
        gtsam::Pose3 pose_to = poseEigToGtsamPose(keyframes_[closest_keyframe_idx].pose_corrected_eig_);
        auto variance_vector = (gtsam::Vector(6) << score, score, score, score, score, score).finished();
        gtsam::noiseModel::Diagonal::shared_ptr loop_noise = gtsam::noiseModel::Diagonal::Variances(variance_vector);
        {
            std::lock_guard<std::mutex> lock(graph_mutex_);
            gtsam_graph_.add(gtsam::BetweenFactor<gtsam::Pose3>(latest_keyframe.idx_,
                                                                closest_keyframe_idx,
                                                                pose_from.between(pose_to),
                                                                loop_noise));
        }
        loop_idx_pairs_.push_back({latest_keyframe.idx_, closest_keyframe_idx}); // for vis
        loop_added_flag_vis_ = true;
        loop_added_flag_ = true;
    }
    else
    {
        ROS_WARN("Loop closure rejected. Score: %.3f", reg_output.score_);
    }
    high_resolution_clock::time_point t2 = high_resolution_clock::now();

    debug_src_pub_.publish(pclToPclRos(loop_closure_->getSourceCloud(), map_frame_));
    debug_dst_pub_.publish(pclToPclRos(loop_closure_->getTargetCloud(), map_frame_));
    debug_fine_aligned_pub_.publish(pclToPclRos(loop_closure_->getFinalAlignedCloud(), map_frame_));

    ROS_INFO("loop: %.1f", duration_cast<microseconds>(t2 - t1).count() / 1e3);
    return;
}

void FastLioSam::visTimerFunc(const ros::TimerEvent &event)
{
    (void)event;
    if (!is_initialized_)
    {
        return;
    }

    high_resolution_clock::time_point tv1 = high_resolution_clock::now();
    //// 1. if loop closed, correct vis data
    if (loop_added_flag_vis_)
    // copy and ready
    {
        gtsam::Values corrected_esti_copied;
        pcl::PointCloud<pcl::PointXYZ> corrected_odoms;
        nav_msgs::Path corrected_path;
        {
            std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
            corrected_esti_copied = corrected_esti_;
        }
        // correct pose and path
        for (size_t i = 0; i < corrected_esti_copied.size(); ++i)
        {
            gtsam::Pose3 pose_ = corrected_esti_copied.at<gtsam::Pose3>(i);
            corrected_odoms.points.emplace_back(pose_.translation().x(), pose_.translation().y(), pose_.translation().z());
            corrected_path.poses.push_back(gtsamPoseToPoseStamped(pose_, map_frame_));
        }
        // update vis of loop constraints
        if (!loop_idx_pairs_.empty())
        {
            loop_detection_pub_.publish(getLoopMarkers(corrected_esti_copied));
        }
        // update with corrected data
        {
            std::lock_guard<std::mutex> lock(vis_mutex_);
            corrected_odoms_ = corrected_odoms;
            corrected_path_.poses = corrected_path.poses;
        }
        loop_added_flag_vis_ = false;
    }
    //// 2. publish odoms, paths
    {
        std::lock_guard<std::mutex> lock(vis_mutex_);
        odom_pub_.publish(pclToPclRos(odoms_, map_frame_));
        path_pub_.publish(odom_path_);
        corrected_odom_pub_.publish(pclToPclRos(corrected_odoms_, map_frame_));
        corrected_path_pub_.publish(corrected_path_);
    }

    //// 3. global map
    if (global_map_vis_switch_ && corrected_pcd_map_pub_.getNumSubscribers() > 0) // save time, only once
    {
        pcl::PointCloud<PointType>::Ptr corrected_map(new pcl::PointCloud<PointType>());
        corrected_map->reserve(keyframes_[0].pcd_.size() * keyframes_.size()); // it's an approximated size
        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                *corrected_map += transformPcd(keyframes_[i].pcd_, keyframes_[i].pose_corrected_eig_);
            }
        }
        const auto &voxelized_map = voxelizePcd(corrected_map, voxel_res_);
        corrected_pcd_map_pub_.publish(pclToPclRos(*voxelized_map, map_frame_));
        global_map_vis_switch_ = false;
    }
    if (!global_map_vis_switch_ && corrected_pcd_map_pub_.getNumSubscribers() == 0)
    {
        global_map_vis_switch_ = true;
    }
    high_resolution_clock::time_point tv2 = high_resolution_clock::now();
    ROS_INFO("vis: %.1fms", duration_cast<microseconds>(tv2 - tv1).count() / 1e3);
    return;
}

void FastLioSam::saveFlagCallback(const std_msgs::String::ConstPtr &msg)
{
    std::string save_dir = msg->data != "" ? msg->data : package_path_;

    // save scans as individual pcd files and poses in KITTI format
    // Delete the scans folder if it exists and create a new one
    std::string seq_directory = save_dir + "/" + seq_name_;
    std::string scans_directory = seq_directory + "/scans";
    if (save_in_kitti_format_)
    {
        ROS_INFO("\033[32;1mScans are saved in %s, following the KITTI and TUM format\033[0m", scans_directory.c_str());
        if (fs::exists(seq_directory))
        {
            fs::remove_all(seq_directory);
        }
        fs::create_directories(scans_directory);

        std::ofstream kitti_pose_file(seq_directory + "/poses_kitti.txt");
        std::ofstream tum_pose_file(seq_directory + "/poses_tum.txt");
        tum_pose_file << "#timestamp x y z qx qy qz qw\n";
        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                // Save the point cloud
                std::stringstream ss_;
                ss_ << scans_directory << "/" << std::setw(6) << std::setfill('0') << i << ".pcd";
                ROS_INFO("Saving %s...", ss_.str().c_str());
                pcl::io::savePCDFileASCII<PointType>(ss_.str(), keyframes_[i].pcd_);

                // Save the pose in KITTI format
                const auto &pose_ = keyframes_[i].pose_corrected_eig_;
                kitti_pose_file << pose_(0, 0) << " " << pose_(0, 1) << " " << pose_(0, 2) << " "
                                << pose_(0, 3) << " " << pose_(1, 0) << " " << pose_(1, 1) << " "
                                << pose_(1, 2) << " " << pose_(1, 3) << " " << pose_(2, 0) << " "
                                << pose_(2, 1) << " " << pose_(2, 2) << " " << pose_(2, 3) << "\n";

                const auto &lidar_optim_pose_ = poseEigToPoseStamped(keyframes_[i].pose_corrected_eig_);
                tum_pose_file << std::fixed << std::setprecision(8) << keyframes_[i].timestamp_
                              << " " << lidar_optim_pose_.pose.position.x << " "
                              << lidar_optim_pose_.pose.position.y << " "
                              << lidar_optim_pose_.pose.position.z << " "
                              << lidar_optim_pose_.pose.orientation.x << " "
                              << lidar_optim_pose_.pose.orientation.y << " "
                              << lidar_optim_pose_.pose.orientation.z << " "
                              << lidar_optim_pose_.pose.orientation.w << "\n";
            }
        }
        kitti_pose_file.close();
        tum_pose_file.close();
        ROS_INFO("\033[32;1mScans and poses saved in .pcd and KITTI format\033[0m");
    }

    if (save_map_bag_)
    {
        rosbag::Bag bag;
        bag.open(package_path_ + "/result/result.bag", rosbag::bagmode::Write);
        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                ros::Time time;
                time.fromSec(keyframes_[i].timestamp_);
                bag.write("/keyframe_pcd", time, pclToPclRos(keyframes_[i].pcd_, map_frame_));
                bag.write("/keyframe_pose", time, poseEigToPoseStamped(keyframes_[i].pose_corrected_eig_));
            }
        }
        bag.close();
        ROS_INFO("\033[36;1mResult saved in .bag format!!!\033[0m");
    }

    if (save_map_pcd_)
    {
        pcl::PointCloud<PointType>::Ptr corrected_map(new pcl::PointCloud<PointType>());
        corrected_map->reserve(keyframes_[0].pcd_.size() * keyframes_.size()); // it's an approximated size
        {
            std::lock_guard<std::mutex> lock(keyframes_mutex_);
            for (size_t i = 0; i < keyframes_.size(); ++i)
            {
                *corrected_map += transformPcd(keyframes_[i].pcd_, keyframes_[i].pose_corrected_eig_);
            }
        }
        const auto &voxelized_map = voxelizePcd(corrected_map, voxel_res_);
        const auto map_to_save = capPointCount(voxelized_map);
        if (map_to_save->size() < voxelized_map->size())
        {
            ROS_WARN("Capped map points before save: %zu -> %zu", voxelized_map->size(), map_to_save->size());
        }
        pcl::io::savePCDFileASCII<PointType>(seq_directory + "/" + seq_name_ + "_map.pcd", *map_to_save);
        ROS_INFO("\033[32;1mAccumulated map cloud saved in .pcd format\033[0m");
    }
}

void FastLioSam::shutdownAndSave()
{
    std::lock_guard<std::mutex> shutdown_lock(shutdown_save_mutex_);
    if (shutdown_saved_)
    {
        ROS_INFO("shutdownAndSave already executed. Skipping duplicate call.");
        return;
    }

    ROS_WARN("shutdownAndSave called. save_map_bag=%d, save_map_pcd=%d", save_map_bag_, save_map_pcd_);

    // Stop incoming callbacks before taking a snapshot for save.
    loop_timer_.stop();
    vis_timer_.stop();
    sub_save_flag_.shutdown();
    if (sub_odom_pcd_sync_)
    {
        sub_odom_pcd_sync_.reset();
    }
    if (sub_odom_)
    {
        sub_odom_->unsubscribe();
        sub_odom_.reset();
    }
    if (sub_pcd_)
    {
        sub_pcd_->unsubscribe();
        sub_pcd_.reset();
    }

    std::vector<PosePcd> keyframes_snapshot;
    {
        std::lock_guard<std::mutex> lock(keyframes_mutex_);
        keyframes_snapshot = keyframes_;
    }

    PosePcd current_frame_snapshot;
    bool has_current_frame = false;
    {
        std::lock_guard<std::mutex> lock(realtime_pose_mutex_);
        current_frame_snapshot = current_frame_;
        has_current_frame = !current_frame_snapshot.pcd_.empty();
    }
    if (has_current_frame &&
        (keyframes_snapshot.empty() ||
         current_frame_snapshot.timestamp_ > keyframes_snapshot.back().timestamp_ + 1e-6))
    {
        keyframes_snapshot.push_back(current_frame_snapshot);
        ROS_WARN("Added latest non-keyframe scan to shutdown snapshot.");
    }

    if (keyframes_snapshot.empty())
    {
        ROS_WARN("No keyframes collected. Skipping shutdown save.");
        shutdown_saved_ = true;
        return;
    }

    ROS_WARN("shutdownAndSave snapshot size: %zu", keyframes_snapshot.size());
    ROS_WARN("shutdownAndSave output directory: %s/result", package_path_.c_str());

    try
    {
        if (save_map_bag_)
        {
            rosbag::Bag bag;
            bag.open(package_path_ + "/result/result.bag", rosbag::bagmode::Write);
            for (size_t i = 0; i < keyframes_snapshot.size(); ++i)
            {
                ros::Time time;
                time.fromSec(keyframes_snapshot[i].timestamp_);
                bag.write("/keyframe_pcd", time, pclToPclRos(keyframes_snapshot[i].pcd_, map_frame_));
                bag.write("/keyframe_pose", time, poseEigToPoseStamped(keyframes_snapshot[i].pose_corrected_eig_));
            }
            bag.close();
            ROS_INFO("\033[36;1mResult saved in .bag format!!!\033[0m");
        }

        if (save_map_pcd_)
        {
            pcl::PointCloud<PointType>::Ptr corrected_map(new pcl::PointCloud<PointType>());
            corrected_map->reserve(keyframes_snapshot[0].pcd_.size() * keyframes_snapshot.size());
            for (size_t i = 0; i < keyframes_snapshot.size(); ++i)
            {
                *corrected_map += transformPcd(keyframes_snapshot[i].pcd_, keyframes_snapshot[i].pose_corrected_eig_);
            }
            const auto &voxelized_map = voxelizePcd(corrected_map, voxel_res_);
            const auto map_to_save = capPointCount(voxelized_map);
            if (map_to_save->size() < voxelized_map->size())
            {
                ROS_WARN("Capped map points before save: %zu -> %zu", voxelized_map->size(), map_to_save->size());
            }
            pcl::io::savePCDFileASCII<PointType>(package_path_ + "/result/result.pcd", *map_to_save);
            ROS_INFO("\033[32;1mResult saved in .pcd format!!!\033[0m");
        }
    }
    catch (const std::exception &e)
    {
        ROS_ERROR("Exception during shutdown save: %s", e.what());
    }
    catch (...)
    {
        ROS_ERROR("Unknown exception during shutdown save");
    }

    if (save_mapped_txt_)
    {
        try
        {
            std::ofstream mapped_txt_file(mapped_txt_path_, std::ios::out);
            if (mapped_txt_file.is_open())
            {
                std::vector<const PosePcd *> poses_to_write;
                poses_to_write.reserve(keyframes_snapshot.size());
                for (const auto &kf : keyframes_snapshot)
                {
                    poses_to_write.push_back(&kf);
                }

                if (match_mapped_txt_baseline_length_ && !mapped_txt_baseline_path_.empty() && !poses_to_write.empty())
                {
                    std::ifstream baseline_file(mapped_txt_baseline_path_);
                    size_t baseline_count = 0;
                    std::string line;
                    while (std::getline(baseline_file, line))
                    {
                        if (!line.empty() && line[0] != '#')
                        {
                            ++baseline_count;
                        }
                    }

                    if (baseline_count == 0)
                    {
                        ROS_WARN("Baseline txt has no valid lines: %s. Writing full mapped txt.", mapped_txt_baseline_path_.c_str());
                    }
                    else if (baseline_count == poses_to_write.size())
                    {
                        ROS_INFO("Mapped txt already matches baseline length: %zu lines", baseline_count);
                    }
                    else
                    {
                        std::vector<const PosePcd *> sampled_poses;
                        sampled_poses.reserve(baseline_count);
                        if (baseline_count == 1)
                        {
                            sampled_poses.push_back(poses_to_write.front());
                        }
                        else
                        {
                            const double step = static_cast<double>(poses_to_write.size() - 1) / static_cast<double>(baseline_count - 1);
                            for (size_t i = 0; i < baseline_count; ++i)
                            {
                                const size_t idx = static_cast<size_t>(std::round(i * step));
                                sampled_poses.push_back(poses_to_write[idx]);
                            }
                        }
                        poses_to_write.swap(sampled_poses);
                        ROS_INFO("Resampled mapped.txt length to baseline: %zu -> %zu (baseline: %s)",
                                 keyframes_snapshot.size(),
                                 poses_to_write.size(),
                                 mapped_txt_baseline_path_.c_str());
                    }
                }

                mapped_txt_file.setf(std::ios::fixed, std::ios::floatfield);
                mapped_txt_file.precision(10);
                for (const auto *kf_ptr : poses_to_write)
                {
                    const auto &kf = *kf_ptr;
                    Eigen::Quaterniond quat(kf.pose_corrected_eig_.block<3, 3>(0, 0));
                    quat.normalize();
                    mapped_txt_file << kf.timestamp_ << " "
                                    << kf.pose_corrected_eig_(0, 3) << " "
                                    << kf.pose_corrected_eig_(1, 3) << " "
                                    << kf.pose_corrected_eig_(2, 3) << " "
                                    << quat.x() << " "
                                    << quat.y() << " "
                                    << quat.z() << " "
                                    << quat.w() << "\n";
                }
                ROS_INFO("\033[36;1mTUM mapped.txt saved: %s (%zu poses)\033[0m", mapped_txt_path_.c_str(), poses_to_write.size());
            }
            else
            {
                ROS_ERROR("Failed to open mapped.txt for write: %s", mapped_txt_path_.c_str());
            }
        }
        catch (const std::exception &e)
        {
            ROS_ERROR("Exception saving mapped.txt: %s", e.what());
        }
    }

    shutdown_saved_ = true;
}

pcl::PointCloud<PointType>::Ptr FastLioSam::capPointCount(const pcl::PointCloud<PointType>::Ptr &cloud) const
{
    if (!cloud || save_max_points_ <= 0)
    {
        return cloud;
    }

    const size_t max_points = static_cast<size_t>(save_max_points_);
    if (cloud->size() <= max_points)
    {
        return cloud;
    }

    pcl::PointCloud<PointType>::Ptr capped_cloud(new pcl::PointCloud<PointType>());
    capped_cloud->points.reserve(max_points);

    if (max_points == 1)
    {
        capped_cloud->points.push_back(cloud->points.front());
    }
    else
    {
        const double step = static_cast<double>(cloud->size() - 1) / static_cast<double>(max_points - 1);
        for (size_t i = 0; i < max_points; ++i)
        {
            const size_t idx = static_cast<size_t>(std::round(i * step));
            capped_cloud->points.push_back(cloud->points[idx]);
        }
    }

    capped_cloud->width = static_cast<uint32_t>(capped_cloud->points.size());
    capped_cloud->height = 1;
    capped_cloud->is_dense = cloud->is_dense;
    return capped_cloud;
}

FastLioSam::~FastLioSam()
{
    shutdownAndSave();
}

void FastLioSam::appendMappedTumPose(double timestamp, const Eigen::Matrix4d &pose_eig)
{
    std::ofstream mapped_txt_file(mapped_txt_path_, std::ios::app);
    if (!mapped_txt_file.is_open())
    {
        ROS_ERROR("Failed to open mapped.txt for write: %s", mapped_txt_path_.c_str());
        return;
    }

    Eigen::Quaterniond quat(pose_eig.block<3, 3>(0, 0));
    quat.normalize();

    mapped_txt_file.setf(std::ios::fixed, std::ios::floatfield);
    mapped_txt_file.precision(10);
    mapped_txt_file << timestamp << " "
                    << pose_eig(0, 3) << " "
                    << pose_eig(1, 3) << " "
                    << pose_eig(2, 3) << " "
                    << quat.x() << " "
                    << quat.y() << " "
                    << quat.z() << " "
                    << quat.w() << std::endl;
}

void FastLioSam::updateOdomsAndPaths(const PosePcd &pose_pcd_in)
{
    odoms_.points.emplace_back(pose_pcd_in.pose_eig_(0, 3),
                               pose_pcd_in.pose_eig_(1, 3),
                               pose_pcd_in.pose_eig_(2, 3));
    corrected_odoms_.points.emplace_back(pose_pcd_in.pose_corrected_eig_(0, 3),
                                         pose_pcd_in.pose_corrected_eig_(1, 3),
                                         pose_pcd_in.pose_corrected_eig_(2, 3));
    odom_path_.poses.emplace_back(poseEigToPoseStamped(pose_pcd_in.pose_eig_, map_frame_));
    corrected_path_.poses.emplace_back(poseEigToPoseStamped(pose_pcd_in.pose_corrected_eig_, map_frame_));
    return;
}

visualization_msgs::Marker FastLioSam::getLoopMarkers(const gtsam::Values &corrected_esti_in)
{
    visualization_msgs::Marker edges;
    edges.type = 5u;
    edges.scale.x = 0.12f;
    edges.header.frame_id = map_frame_;
    edges.pose.orientation.w = 1.0f;
    edges.color.r = 1.0f;
    edges.color.g = 1.0f;
    edges.color.b = 1.0f;
    edges.color.a = 1.0f;
    for (size_t i = 0; i < loop_idx_pairs_.size(); ++i)
    {
        if (loop_idx_pairs_[i].first >= corrected_esti_in.size() ||
            loop_idx_pairs_[i].second >= corrected_esti_in.size())
        {
            continue;
        }
        gtsam::Pose3 pose = corrected_esti_in.at<gtsam::Pose3>(loop_idx_pairs_[i].first);
        gtsam::Pose3 pose2 = corrected_esti_in.at<gtsam::Pose3>(loop_idx_pairs_[i].second);
        geometry_msgs::Point p, p2;
        p.x = pose.translation().x();
        p.y = pose.translation().y();
        p.z = pose.translation().z();
        p2.x = pose2.translation().x();
        p2.y = pose2.translation().y();
        p2.z = pose2.translation().z();
        edges.points.push_back(p);
        edges.points.push_back(p2);
    }
    return edges;
}

bool FastLioSam::checkIfKeyframe(const PosePcd &pose_pcd_in, const PosePcd &latest_pose_pcd)
{
    return keyframe_thr_ < (latest_pose_pcd.pose_corrected_eig_.block<3, 1>(0, 3) - pose_pcd_in.pose_corrected_eig_.block<3, 1>(0, 3)).norm();
}
