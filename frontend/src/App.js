import { useEffect, useState } from "react";
import "./App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import axios from "axios";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api`;

const Dashboard = () => {
  const [stats, setStats] = useState(null);
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);
  const [jobs, setJobs] = useState([]);
  const [scrapingConfig, setScrapingConfig] = useState(null);
  const [startingJob, setStartingJob] = useState(false);

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchDashboardData, 10000); // Refresh every 10 seconds
    return () => clearInterval(interval);
  }, []);

  const fetchDashboardData = async () => {
    try {
      const [statsRes, healthRes, jobsRes, configRes] = await Promise.all([
        axios.get(`${API}/dashboard/stats`),
        axios.get(`${API}/dashboard/health`),
        axios.get(`${API}/scraping/jobs`),
        axios.get(`${API}/scraping/config`)
      ]);

      setStats(statsRes.data);
      setHealth(healthRes.data);
      setJobs(jobsRes.data);
      setScrapingConfig(configRes.data);
      setLoading(false);
    } catch (error) {
      console.error("Error fetching dashboard data:", error);
      setLoading(false);
    }
  };

  const startScrapingJob = async (category, targetCount = 500) => {
    try {
      setStartingJob(true);
      const jobData = {
        job_name: `IndiaBix ${category} Scraping`,
        categories: [category],
        target_count: targetCount
      };

      const response = await axios.post(`${API}/scraping/start`, jobData);
      
      alert(`Scraping job started successfully! Job ID: ${response.data.job_id}\nEstimated Duration: ${response.data.estimated_duration}`);
      
      // Refresh data after starting job
      setTimeout(fetchDashboardData, 2000);
      
    } catch (error) {
      console.error("Error starting scraping job:", error);
      alert(`Failed to start scraping job: ${error.response?.data?.detail || error.message}`);
    } finally {
      setStartingJob(false);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-900 flex items-center justify-center">
        <div className="text-white text-xl">Loading dashboard...</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      <header className="mb-8">
        <h1 className="text-4xl font-bold text-cyan-400 mb-2">
          🎯 Aptitude Question Bank
        </h1>
        <p className="text-gray-300 text-lg">
          Advanced IndiaBix Scraping & Question Management System
        </p>
      </header>

      {/* System Health Status */}
      {health && (
        <div className="mb-8 grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className={`p-4 rounded-lg border-2 ${
            health.database_status === 'healthy' 
              ? 'border-green-500 bg-green-900/20' 
              : 'border-red-500 bg-red-900/20'
          }`}>
            <h3 className="font-semibold mb-1">Database</h3>
            <p className={`text-sm ${
              health.database_status === 'healthy' ? 'text-green-400' : 'text-red-400'
            }`}>
              {health.database_status.toUpperCase()}
            </p>
          </div>
          
          <div className={`p-4 rounded-lg border-2 ${
            health.chrome_driver_status === 'healthy' 
              ? 'border-green-500 bg-green-900/20' 
              : 'border-red-500 bg-red-900/20'
          }`}>
            <h3 className="font-semibold mb-1">Chrome Driver</h3>
            <p className={`text-sm ${
              health.chrome_driver_status === 'healthy' ? 'text-green-400' : 'text-red-400'
            }`}>
              {health.chrome_driver_status.toUpperCase()}
            </p>
          </div>
          
          <div className={`p-4 rounded-lg border-2 ${
            health.scraping_service_status === 'idle' 
              ? 'border-blue-500 bg-blue-900/20' 
              : 'border-yellow-500 bg-yellow-900/20'
          }`}>
            <h3 className="font-semibold mb-1">Scraping Service</h3>
            <p className={`text-sm ${
              health.scraping_service_status === 'idle' ? 'text-blue-400' : 'text-yellow-400'
            }`}>
              {health.scraping_service_status.toUpperCase()}
            </p>
            {health.active_connections > 0 && (
              <p className="text-xs text-gray-400">
                Active jobs: {health.active_connections}
              </p>
            )}
          </div>
        </div>
      )}

      {/* Dashboard Stats */}
      {stats && (
        <div className="mb-8 grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-gray-800 p-6 rounded-lg border border-gray-700">
            <h3 className="text-sm text-gray-400 mb-1">Total Questions</h3>
            <p className="text-3xl font-bold text-cyan-400">{stats.total_questions.toLocaleString()}</p>
          </div>
          
          <div className="bg-gray-800 p-6 rounded-lg border border-gray-700">
            <h3 className="text-sm text-gray-400 mb-1">Categories</h3>
            <p className="text-3xl font-bold text-purple-400">{stats.categories_covered}</p>
          </div>
          
          <div className="bg-gray-800 p-6 rounded-lg border border-gray-700">
            <h3 className="text-sm text-gray-400 mb-1">Active Jobs</h3>
            <p className="text-3xl font-bold text-yellow-400">{stats.active_jobs}</p>
          </div>
          
          <div className="bg-gray-800 p-6 rounded-lg border border-gray-700">
            <h3 className="text-sm text-gray-400 mb-1">Avg Quality</h3>
            <p className="text-3xl font-bold text-green-400">{stats.avg_quality_score}%</p>
          </div>
        </div>
      )}

      {/* Quick Start Scraping */}
      {scrapingConfig && (
        <div className="mb-8">
          <h2 className="text-2xl font-bold mb-4 text-cyan-400">🚀 Quick Start Scraping</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {Object.entries(scrapingConfig.category_details).map(([categoryKey, categoryInfo]) => (
              <div key={categoryKey} className="bg-gray-800 p-6 rounded-lg border border-gray-700">
                <h3 className="text-lg font-semibold mb-2 text-white">
                  {categoryInfo.display_name}
                </h3>
                <p className="text-sm text-gray-400 mb-3">
                  {categoryInfo.subcategories.length} subcategories
                </p>
                <p className="text-xs text-gray-500 mb-4">
                  Target: {categoryInfo.total_target.toLocaleString()} questions
                </p>
                <button
                  onClick={() => startScrapingJob(categoryKey, 500)}
                  disabled={startingJob}
                  className={`w-full py-2 px-4 rounded-md font-medium transition-colors ${
                    startingJob 
                      ? 'bg-gray-600 text-gray-400 cursor-not-allowed' 
                      : 'bg-cyan-600 text-white hover:bg-cyan-700'
                  }`}
                >
                  {startingJob ? 'Starting...' : 'Start Scraping'}
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Jobs */}
      {jobs.length > 0 && (
        <div className="mb-8">
          <h2 className="text-2xl font-bold mb-4 text-cyan-400">📋 Recent Scraping Jobs</h2>
          <div className="bg-gray-800 rounded-lg border border-gray-700 overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-700">
                  <tr>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Job Name</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Status</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Progress</th>
                    <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Created</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.slice(0, 10).map((job, index) => (
                    <tr key={job.id} className={index % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'}>
                      <td className="px-4 py-3 text-sm text-white">{job.job_name}</td>
                      <td className="px-4 py-3 text-sm">
                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                          job.status === 'completed' ? 'bg-green-900 text-green-300' :
                          job.status === 'in_progress' ? 'bg-yellow-900 text-yellow-300' :
                          job.status === 'failed' ? 'bg-red-900 text-red-300' :
                          'bg-gray-700 text-gray-300'
                        }`}>
                          {job.status.replace('_', ' ').toUpperCase()}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-300">
                        {job.questions_saved} / {job.target_count} 
                        {job.success_rate > 0 && (
                          <span className="text-green-400 ml-2">({job.success_rate}%)</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-400">
                        {new Date(job.created_at).toLocaleDateString()}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}

      {/* Category Distribution */}
      {stats && stats.category_distribution && Object.keys(stats.category_distribution).length > 0 && (
        <div className="mb-8">
          <h2 className="text-2xl font-bold mb-4 text-cyan-400">📊 Question Distribution</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-gray-800 p-6 rounded-lg border border-gray-700">
              <h3 className="text-lg font-semibold mb-4 text-white">By Category</h3>
              <div className="space-y-3">
                {Object.entries(stats.category_distribution).map(([category, count]) => (
                  <div key={category} className="flex justify-between items-center">
                    <span className="text-gray-300 capitalize">
                      {category.replace('_', ' ')}
                    </span>
                    <span className="text-cyan-400 font-semibold">
                      {count.toLocaleString()}
                    </span>
                  </div>
                ))}
              </div>
            </div>
            
            {stats.difficulty_distribution && (
              <div className="bg-gray-800 p-6 rounded-lg border border-gray-700">
                <h3 className="text-lg font-semibold mb-4 text-white">By Difficulty</h3>
                <div className="space-y-3">
                  {Object.entries(stats.difficulty_distribution).map(([difficulty, count]) => (
                    <div key={difficulty} className="flex justify-between items-center">
                      <span className={`capitalize font-medium ${
                        difficulty === 'easy' ? 'text-green-400' :
                        difficulty === 'medium' ? 'text-yellow-400' :
                        'text-red-400'
                      }`}>
                        {difficulty}
                      </span>
                      <span className="text-white font-semibold">
                        {count.toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <footer className="text-center text-gray-500 text-sm">
        <p>🎯 Aptitude Question Bank - IndiaBix Scraping System v1.0</p>
        <p>Built with FastAPI, React, and MongoDB</p>
      </footer>
    </div>
  );
};

function App() {
  return (
    <div className="App">
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Dashboard />}>
            <Route index element={<Dashboard />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </div>
  );
}

export default App;